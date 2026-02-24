use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_account_decoder::{UiAccount, UiAccountData, UiAccountEncoding};
use solana_client::rpc_config::{
    RpcSimulateTransactionAccountsConfig, RpcSimulateTransactionConfig,
};
use solana_client::rpc_response::{Response, RpcSimulateTransactionResult};
use solana_address_lookup_table_interface::state::AddressLookupTable;
use solana_hash::Hash;
use solana_instruction::Instruction;
use solana_keypair::{read_keypair_file, Keypair};
use solana_message::AddressLookupTableAccount;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use solana_transaction_status::{UiInstruction, UiParsedInstruction};
use solana_transaction::versioned::VersionedTransaction;
use tokio::time::{timeout, Duration};

use crate::{
    constants::METEORA_LB_PROGRAM_ID,
    jito_grpc::JitoGrpcClient,
    log_utils::append_trade_log,
    rpc::AppContext,
    sidecar_client::{Side, SidecarClient},
    DexKind,
    TradeSide,
};

use super::{
    arbitrage::AtomicTradePlan,
    tx::{build_atomic_transaction, build_tip_instruction, AtomicTxContext, Bundle},
    TradeAccounts, TradePlan,
};

const TOKEN_2022_PROGRAM_ID: Pubkey =
    solana_pubkey::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");

#[derive(Clone)]
struct TipSettings {
    receiver: Option<Pubkey>,
}

pub struct AtomicExecutor {
    ctx: AppContext,
    signer: Arc<Keypair>,
    tip: Option<TipSettings>,
    jito: Option<Arc<JitoClient>>,
    jito_grpc: Option<Arc<JitoGrpcClient>>,
    lookup_tables: std::sync::RwLock<Vec<AddressLookupTableAccount>>,
}

#[derive(Debug, Clone, Copy)]
pub struct SimSwapSummary {
    pub quote_spent_ui: Option<f64>,
    pub base_received_ui: Option<f64>,
    pub slippage_pct: Option<f64>,
    pub slippage_bps: Option<f64>,
}

impl AtomicExecutor {
    pub async fn from_env(ctx: AppContext) -> Result<Option<Self>> {
        let path = match std::env::var("SOLANA_SIGNER_KEYPAIR") {
            Ok(value) if !value.trim().is_empty() => value,
            _ => return Ok(None),
        };

        let signer = Arc::new(
            read_keypair_file(&path)
                .map_err(|err| anyhow!("load SOLANA_SIGNER_KEYPAIR {path}: {err}"))?,
        );

        let tip = load_tip_settings()?;
        let jito = match std::env::var("JITO_RELAY_URL") {
            Ok(url) if !url.trim().is_empty() => Some(Arc::new(JitoClient::new(url)?)),
            _ => None,
        };
        let jito_grpc = JitoGrpcClient::from_env().await?.map(Arc::new);

        let lookup_tables = load_lookup_tables(&ctx).await?;
        if jito.is_some() || jito_grpc.is_some() {
            if let Err(err) = refresh_jito_tip_account(&ctx).await {
                warn!("[JITO] Failed to refresh tip account cache at startup: {:?}", err);
            }
        }

        Ok(Some(Self {
            ctx,
            signer,
            tip,
            jito,
            jito_grpc,
            lookup_tables: std::sync::RwLock::new(lookup_tables),
        }))
    }

    pub fn signer_pubkey(&self) -> Pubkey {
        self.signer.pubkey()
    }

    /// 返回所有配置的 ALT
    /// 
    /// 为保证 simulate 和 submit 行为一致，统一使用全部 ALT。
    /// 这样可以最大程度利用 ALT 压缩交易大小，且避免因 ALT 选择差异导致的问题。
    fn select_alts_for_plan(&self, _plan: &AtomicTradePlan) -> Vec<AddressLookupTableAccount> {
        // ✅ 直接返回全部 ALT，与 simulate 保持一致
        match self.lookup_tables.read() {
            Ok(guard) => guard.clone(),
            Err(_) => {
                warn!("Lookup table cache lock poisoned; returning empty ALT list");
                Vec::new()
            }
        }
    }

    /// 重新加载 ALT（用于 ALT 扩展后刷新本地缓存）
    pub async fn refresh_lookup_tables(&self) -> Result<usize> {
        let tables = load_lookup_tables(&self.ctx).await?;
        let count = tables.len();
        let mut guard = self
            .lookup_tables
            .write()
            .map_err(|_| anyhow!("lookup table cache lock poisoned"))?;
        *guard = tables;
        Ok(count)
    }

    pub async fn submit_plan(&self, plan: &AtomicTradePlan) -> Result<SubmissionSummary> {
        for attempt in 0..2 {
            let blockhash = if attempt == 0 {
                // ✅ 使用缓存的 blockhash，避免网络延迟
                self.ctx
                    .get_blockhash()
                    .await
                    .context("fetch blockhash for atomic trade")?
            } else {
                // ✅ 失败后强制刷新 blockhash 再试一次
                self.ctx
                    .get_blockhash_fresh()
                    .await
                    .context("refresh blockhash for atomic trade")?
            };

            let built = self.build_bundle(plan, blockhash)?;
            let signatures = extract_signatures(&built.transactions);
            let base64 = built.base64.clone();

            let submit_result = if let Some(client) = &self.jito_grpc {
                client.send_bundle(&built.transactions).await.map(|bundle_id| {
                    info!(
                        "Submitted bundle with {} transactions via gRPC {} (bundle_id: {})",
                        built.transactions.len(),
                        client.endpoint(),
                        bundle_id
                    );
                    SubmissionSummary {
                        bundle_base64: base64,
                        signatures,
                        relay: Some(client.endpoint().to_string()),
                        bundle_id: Some(bundle_id),
                    }
                })
            } else if let Some(client) = &self.jito {
                client.send_bundle(&base64).await.map(|bundle_id| {
                    info!(
                        "Submitted bundle with {} transactions via {} (bundle_id: {})",
                        base64.len(),
                        client.endpoint(),
                        bundle_id
                    );
                    SubmissionSummary {
                        bundle_base64: base64,
                        signatures,
                        relay: Some(client.endpoint().to_string()),
                        bundle_id: Some(bundle_id),
                    }
                })
            } else {
                bail!(
                    "Jito relay not configured (JITO_GRPC_URL/JITO_RELAY_URL missing); aborting submission"
                );
            };

            match submit_result {
                Ok(summary) => return Ok(summary),
                Err(err) => {
                    if attempt == 0 && is_expired_blockhash(&err) {
                        warn!(
                            "Jito submission failed with expired blockhash; retrying with fresh blockhash"
                        );
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        bail!("submit bundle failed after blockhash refresh retry");
    }

    /// 在链上 dry-run 原子套利计划：构建一笔 v0 Transaction，
    /// 记录 simulateTransaction 的返回结果到交易日志。
    pub async fn simulate_atomic_plan(
        &self,
        plan: &AtomicTradePlan,
        buy_accounts: &TradeAccounts,
        sell_accounts: &TradeAccounts,
        trade_id: u64,
    ) -> Result<()> {
        self.simulate_atomic_plan_with_summary(plan, buy_accounts, sell_accounts, trade_id, true)
            .await
            .map(|_| ())
    }

    pub async fn simulate_atomic_plan_with_summary(
        &self,
        plan: &AtomicTradePlan,
        buy_accounts: &TradeAccounts,
        sell_accounts: &TradeAccounts,
        trade_id: u64,
        log_to_trade_log: bool,
    ) -> Result<Option<SimSwapSummary>> {
        let payer = self.signer.pubkey();

        // 需要跟踪的用户 token 账户：买腿/卖腿的 base & quote
        let mut tracked: Vec<Pubkey> = vec![
            payer,
            buy_accounts.user_base_account,
            buy_accounts.user_quote_account,
            sell_accounts.user_base_account,
            sell_accounts.user_quote_account,
        ];
        tracked.retain(|k| *k != Pubkey::default());
        tracked.sort();
        tracked.dedup();

        if tracked.is_empty() {
            warn!("simulate_atomic_plan: no user token accounts configured (TRADE_ACCOUNTS empty?)");
            return Ok(None);
        }

        #[derive(Clone, Copy, Default)]
        struct SimBalance {
            mint: Option<Pubkey>,
            amount: Option<u64>,
        }

        let decode_token_account = |owner: &Pubkey, data: &[u8]| -> Option<SimBalance> {
            if *owner != spl_token::ID && *owner != TOKEN_2022_PROGRAM_ID {
                return None;
            }
            if data.len() < 72 {
                return None;
            }
            let mint: [u8; 32] = data.get(0..32)?.try_into().ok()?;
            let amount_bytes: [u8; 8] = data.get(64..72)?.try_into().ok()?;
            Some(SimBalance {
                mint: Some(Pubkey::new_from_array(mint)),
                amount: Some(u64::from_le_bytes(amount_bytes)),
            })
        };

        let decode_ui_account = |ui: &UiAccount| -> Option<SimBalance> {
            let owner = Pubkey::from_str(&ui.owner).ok()?;
            let data = ui.data.decode()?;
            decode_token_account(&owner, &data)
        };

        let mut pre_balances: HashMap<Pubkey, SimBalance> = HashMap::new();
        let pre_accounts = match timeout(
            Duration::from_secs(5),
            self.ctx.rpc_client().get_multiple_accounts(&tracked),
        )
        .await
        {
            Ok(Ok(accounts)) => Some(accounts),
            Ok(Err(err)) => {
                warn!("simulate_atomic_plan: pre-balance fetch failed: {}", err);
                None
            }
            Err(_) => {
                warn!("simulate_atomic_plan: pre-balance fetch timed out");
                None
            }
        };

        if let Some(pre_accounts) = pre_accounts {
            for (idx, key) in tracked.iter().enumerate() {
                if let Some(Some(account)) = pre_accounts.get(idx) {
                    if let Some(decoded) = decode_token_account(&account.owner, &account.data) {
                        pre_balances.insert(*key, decoded);
                    }
                }
            }
        }

        // ✅ 使用缓存的 blockhash，避免网络延迟
        let blockhash = self.ctx.get_blockhash().await
            .context("fetch blockhash for atomic simulate")?;

        let accounts_cfg = RpcSimulateTransactionAccountsConfig {
            encoding: Some(UiAccountEncoding::Base64),
            addresses: tracked.iter().map(|k| k.to_string()).collect(),
        };

        let sim_config = RpcSimulateTransactionConfig {
            sig_verify: false,
            commitment: None,
            replace_recent_blockhash: true,
            accounts: Some(accounts_cfg),
            min_context_slot: None,
            inner_instructions: true,
            encoding: None,
        };

        // ✅ Dry-run simulate should skip tip to avoid balance failures
        let dry_run = std::env::var("ARBITRAGE_DRY_RUN_ONLY")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        let plan_for_sim = if dry_run && plan.tip_lamports > 0 {
            let mut cloned = plan.clone();
            cloned.tip_lamports = 0;
            Some(cloned)
        } else {
            None
        };

        // ✅ 使用 build_bundle 构建交易，确保与 submit_plan 完全一致（包含正确的 ALT）
        let build_start = std::time::Instant::now();
        let built_bundle = self.build_bundle(plan_for_sim.as_ref().unwrap_or(plan), blockhash)?;
        let build_elapsed = build_start.elapsed();
        if build_elapsed > Duration::from_millis(200) {
            warn!(
                "SIM build_bundle slow (trade_id={}): {:?}",
                trade_id, build_elapsed
            );
        }
        
        // Atomic bundle 应该只包含一笔交易
        let vtx = built_bundle.transactions.first()
            .ok_or_else(|| anyhow!("build_bundle returned empty transaction list"))?
            .clone();

        let sim_start = std::time::Instant::now();
        let response: Response<RpcSimulateTransactionResult> = self.ctx
            .rpc_client()
            .simulate_transaction_with_config(&vtx, sim_config)
            .await
            .context("simulate atomic arbitrage transaction (v0)")?;
        let sim_elapsed = sim_start.elapsed();
        if sim_elapsed > Duration::from_secs(2) {
            warn!(
                "SIM rpc slow (trade_id={}): {:?}",
                trade_id, sim_elapsed
            );
        }
        if log_to_trade_log {
            append_trade_log(&format!(
                "SIM_RPC trade_id={} build_ms={} rpc_ms={}",
                trade_id,
                build_elapsed.as_millis(),
                sim_elapsed.as_millis()
            ));
        }

        let sim_slot = response.context.slot;
        let result = response.value;
        let format_logs = |logs: &Option<Vec<String>>| -> String {
            match logs {
                Some(lines) if !lines.is_empty() => {
                    lines.iter().map(|line| format!("  {line}")).collect::<Vec<_>>().join("\n")
                }
                _ => "  <none>".to_string(),
            }
        };
        let format_return_data = |return_data: &Option<solana_transaction_status::UiTransactionReturnData>| -> String {
            match return_data {
                Some(data) => {
                    let (blob, encoding) = &data.data;
                    let preview = if blob.len() > 120 {
                        format!("{}... (len={})", &blob[..120], blob.len())
                    } else {
                        blob.clone()
                    };
                    format!(
                        "program_id={} encoding={:?} data={}",
                        data.program_id, encoding, preview
                    )
                }
                None => "None".to_string(),
            }
        };
        let format_accounts = |tracked: &[Pubkey], accounts: &Option<Vec<Option<UiAccount>>>| -> String {
            let Some(accounts) = accounts else {
                return "  <none>".to_string();
            };
            let mut lines = Vec::new();
            for (idx, key) in tracked.iter().enumerate() {
                match accounts.get(idx) {
                    Some(Some(ui)) => {
                        let data_desc = match &ui.data {
                            UiAccountData::LegacyBinary(blob) => format!("legacy len={}", blob.len()),
                            UiAccountData::Binary(blob, encoding) => {
                                format!("{:?} len={}", encoding, blob.len())
                            }
                            UiAccountData::Json(_) => "json".to_string(),
                        };
                        lines.push(format!(
                            "  {} owner={} lamports={} executable={} rent_epoch={} space={:?} data={}",
                            key,
                            ui.owner,
                            ui.lamports,
                            ui.executable,
                            ui.rent_epoch,
                            ui.space,
                            data_desc
                        ));
                    }
                    Some(None) => {
                        lines.push(format!("  {} <missing>", key));
                    }
                    None => {
                        lines.push(format!("  {} <not returned>", key));
                    }
                }
            }
            if lines.is_empty() {
                "  <none>".to_string()
            } else {
                lines.join("\n")
            }
        };
        let extract_reason = |logs: &Option<Vec<String>>| -> Option<String> {
            let lines = logs.as_ref()?;
            if let Some(line) = lines.iter().rev().find(|l| l.contains("Error Message:")) {
                return Some(line.to_string());
            }
            if let Some(line) = lines.iter().rev().find(|l| l.contains("AnchorError")) {
                return Some(line.to_string());
            }
            if let Some(line) = lines.iter().rev().find(|l| l.contains("Program log:")) {
                return Some(line.to_string());
            }
            lines.last().cloned()
        };
        if let Some(err) = result.err.as_ref() {
            warn!("Atomic simulate returned error: {:?}", err);
            if let Some(logs) = result.logs.as_ref() {
                for line in logs {
                    warn!("  [sim] {line}");
                }
            }
            
            let err_text = format!("{:?}", err);
            let is_bin_array_err = super::meteora_dlmm::is_bin_array_error(&err.clone().into());
            let is_tick_array_err =
                err_text.contains("Custom(6027)") || err_text.contains("Custom(6028)");

            // ⭐ 检测 bin_array 相关错误并触发后台刷新
            if is_bin_array_err {
                warn!("Detected bin_array error (3005/3007), triggering background refresh for DLMM pools");
                let ctx = self.ctx.clone();
                let buy_pool = plan.buy_pool;
                let sell_pool = plan.sell_pool;
                tokio::spawn(async move {
                    // 刷新 buy 池的 bin_arrays
                    match super::meteora_dlmm::refresh_pool_cached_bin_arrays(&ctx, &buy_pool).await {
                        Ok(count) if count > 0 => {
                            info!("Refreshed {} bin_arrays for buy pool {}", count, buy_pool);
                        }
                        Ok(_) => {}
                        Err(e) => {
                            debug!("Refresh bin_arrays for buy pool {} failed: {}", buy_pool, e);
                        }
                    }
                    // 刷新 sell 池的 bin_arrays（如果不同于 buy 池）
                    if sell_pool != buy_pool {
                        match super::meteora_dlmm::refresh_pool_cached_bin_arrays(&ctx, &sell_pool).await {
                            Ok(count) if count > 0 => {
                                info!("Refreshed {} bin_arrays for sell pool {}", count, sell_pool);
                            }
                            Ok(_) => {}
                            Err(e) => {
                                debug!("Refresh bin_arrays for sell pool {} failed: {}", sell_pool, e);
                            }
                        }
                    }
                });
            }

            // ⭐ 检测 tick array 相关错误并触发后台刷新
            if is_tick_array_err {
                warn!("Detected tick array error (6027/6028), triggering background refresh for CLMM pools");
                let ctx = self.ctx.clone();
                let buy_pool = plan.buy_pool;
                let sell_pool = plan.sell_pool;
                let buy_kind = plan.buy_dex_kind;
                let sell_kind = plan.sell_dex_kind;
                tokio::spawn(async move {
                    match buy_kind {
                        crate::DexKind::OrcaWhirlpool => {
                            let _ = crate::trade::orca::refresh_pool_cached_tick_arrays(&ctx, &buy_pool).await;
                        }
                        crate::DexKind::RaydiumClmm => {
                            let _ = crate::trade::raydium::refresh_pool_cached_tick_arrays(&ctx, &buy_pool).await;
                        }
                        _ => {}
                    }
                    if sell_pool != buy_pool {
                        match sell_kind {
                            crate::DexKind::OrcaWhirlpool => {
                                let _ = crate::trade::orca::refresh_pool_cached_tick_arrays(&ctx, &sell_pool).await;
                            }
                            crate::DexKind::RaydiumClmm => {
                                let _ = crate::trade::raydium::refresh_pool_cached_tick_arrays(&ctx, &sell_pool).await;
                            }
                            _ => {}
                        }
                    }
                });
            }

            let dlmm_debug = if is_bin_array_err {
                Some(collect_dlmm_bin_debug(plan).await)
            } else {
                None
            };
            let dlmm_section = dlmm_debug
                .map(|s| format!("\nDLMM_BIN_DEBUG:\n{}\n", s))
                .unwrap_or_default();

            // ⭐ 程序错误时，记录 FAILED 日志并返回错误（不继续到 SIM_OK）
            let error_msg = err_text;
            let reason = extract_reason(&result.logs).unwrap_or_else(|| "None".to_string());
            if log_to_trade_log {
                let fail_log = format!(
                    "\n========== TRADE #{} ❌ ==========\n\
                     STATUS: ❌ SIM_FAILED (program error)\n\
                     ERROR: {}\n\
                     REASON: {}\n\
                     POOLS: buy={} sell={}\n\
                     BUY:  {}\n\
                     SELL: {}\n\
                     {}\
                     =====================================",
                    trade_id,
                    error_msg.chars().take(150).collect::<String>(),
                    reason,
                    plan.buy_pool,
                    plan.sell_pool,
                    plan.buy_plan.description,
                    plan.sell_plan.description,
                    dlmm_section,
                );
                append_trade_log(&fail_log);
            }
            
            return Err(anyhow::anyhow!("Simulation failed with program error: {}", error_msg));
        }

        let sim_logs = format_logs(&result.logs);
        let return_data = format_return_data(&result.return_data);
        let units = result.units_consumed.map(|v| v.to_string()).unwrap_or_else(|| "None".to_string());
        let accounts_info = format_accounts(&tracked, &result.accounts);

        let mut post_balances: HashMap<Pubkey, SimBalance> = HashMap::new();
        if let Some(accounts) = result.accounts.as_ref() {
            for (idx, key) in tracked.iter().enumerate() {
                if let Some(Some(ui)) = accounts.get(idx) {
                    if let Some(decoded) = decode_ui_account(ui) {
                        post_balances.insert(*key, decoded);
                    }
                }
            }
        }

        let format_amount = |value: Option<u64>, decimals: Option<u8>| -> String {
            match value {
                Some(v) => {
                    if let Some(decimals) = decimals {
                        let scale = 10f64.powi(decimals as i32);
                        if scale.is_finite() && scale > 0.0 {
                            let ui = v as f64 / scale;
                            format!("{} ({:.9})", v, ui)
                        } else {
                            v.to_string()
                        }
                    } else {
                        v.to_string()
                    }
                }
                None => "N/A".to_string(),
            }
        };
        let format_signed_amount = |value: Option<i128>, decimals: Option<u8>| -> String {
            match value {
                Some(v) => {
                    if let Some(decimals) = decimals {
                        let scale = 10f64.powi(decimals as i32);
                        if scale.is_finite() && scale > 0.0 {
                            let ui = v as f64 / scale;
                            return format!("{} ({:+.9})", v, ui);
                        }
                    }
                    v.to_string()
                }
                None => "N/A".to_string(),
            }
        };

        let format_f64 = |value: Option<f64>, digits: usize| -> String {
            match value {
                Some(v) if v.is_finite() => format!("{:.1$}", v, digits),
                _ => "N/A".to_string(),
            }
        };
        #[derive(Clone, Copy, Default)]
        struct TokenFlow {
            out: u64,
            in_: u64,
        }
        let instruction_matches = |left: &Instruction, right: &Instruction| -> bool {
            if left.program_id != right.program_id {
                return false;
            }
            if left.data != right.data {
                return false;
            }
            if left.accounts.len() != right.accounts.len() {
                return false;
            }
            left.accounts.iter().zip(right.accounts.iter()).all(|(a, b)| {
                a.pubkey == b.pubkey && a.is_signer == b.is_signer && a.is_writable == b.is_writable
            })
        };
        let find_instruction_range =
            |all: &[Instruction], target: &[Instruction]| -> Option<std::ops::Range<usize>> {
                if target.is_empty() || all.len() < target.len() {
                    return None;
                }
                for start in 0..=all.len() - target.len() {
                    let window = &all[start..start + target.len()];
                    if window
                        .iter()
                        .zip(target.iter())
                        .all(|(a, b)| instruction_matches(a, b))
                    {
                        return Some(start..start + target.len());
                    }
                }
                None
            };
        let extract_token_transfer = |ix: &UiInstruction| -> Option<(String, String, u64)> {
            let parsed = match ix {
                UiInstruction::Parsed(UiParsedInstruction::Parsed(p)) => p,
                _ => return None,
            };
            if !parsed.program.starts_with("spl-token") {
                return None;
            }
            let obj = parsed.parsed.as_object()?;
            let ix_type = obj.get("type")?.as_str()?;
            let info = obj.get("info")?.as_object()?;
            let source = info.get("source")?.as_str()?.to_string();
            let destination = info.get("destination")?.as_str()?.to_string();
            let amount_str = match ix_type {
                "transfer" => info.get("amount")?.as_str()?,
                "transferChecked" => info
                    .get("tokenAmount")
                    .and_then(Value::as_object)
                    .and_then(|v| v.get("amount"))
                    .and_then(Value::as_str)?,
                _ => return None,
            };
            let amount = amount_str.parse::<u64>().ok()?;
            Some((source, destination, amount))
        };
        let collect_token_flows = |token_key: &Pubkey| -> HashMap<u8, TokenFlow> {
            let mut flows: HashMap<u8, TokenFlow> = HashMap::new();
            let token_key_str = token_key.to_string();
            if let Some(inner_list) = result.inner_instructions.as_ref() {
                for entry in inner_list {
                    let idx = entry.index;
                    for ix in &entry.instructions {
                        if let Some((source, dest, amount)) = extract_token_transfer(ix) {
                            if source == token_key_str || dest == token_key_str {
                                let flow = flows.entry(idx).or_default();
                                if source == token_key_str {
                                    flow.out = flow.out.saturating_add(amount);
                                }
                                if dest == token_key_str {
                                    flow.in_ = flow.in_.saturating_add(amount);
                                }
                            }
                        }
                    }
                }
            }
            flows
        };
        let sum_flows = |flows: &HashMap<u8, TokenFlow>, range: &std::ops::Range<usize>| -> TokenFlow {
            let mut flow = TokenFlow::default();
            for idx in range.clone() {
                if let Some(entry) = flows.get(&(idx as u8)) {
                    flow.out = flow.out.saturating_add(entry.out);
                    flow.in_ = flow.in_.saturating_add(entry.in_);
                }
            }
            flow
        };
        let amount_to_ui = |value: Option<i128>, decimals: Option<u8>| -> Option<f64> {
            let v = value?;
            let d = decimals?;
            let scale = 10f64.powi(d as i32);
            if scale.is_finite() && scale > 0.0 {
                Some(v as f64 / scale)
            } else {
                None
            }
        };

        let mut sim_summary: Option<SimSwapSummary> = None;
        let swap_summary = {
            let base_key = buy_accounts.user_base_account;
            let quote_key = buy_accounts.user_quote_account;
            if base_key == Pubkey::default() || quote_key == Pubkey::default() {
                String::new()
            } else {
                let pre_base = pre_balances.get(&base_key).and_then(|b| b.amount);
                let post_base = post_balances.get(&base_key).and_then(|b| b.amount);
                let pre_quote = pre_balances.get(&quote_key).and_then(|b| b.amount);
                let post_quote = post_balances.get(&quote_key).and_then(|b| b.amount);

                let base_mint = pre_balances
                    .get(&base_key)
                    .and_then(|b| b.mint)
                    .or_else(|| post_balances.get(&base_key).and_then(|b| b.mint));
                let quote_mint = pre_balances
                    .get(&quote_key)
                    .and_then(|b| b.mint)
                    .or_else(|| post_balances.get(&quote_key).and_then(|b| b.mint));

                let base_received = match (pre_base, post_base) {
                    (Some(pre), Some(post)) => Some(post.saturating_sub(pre)),
                    _ => None,
                };
                let quote_spent = match (pre_quote, post_quote) {
                    (Some(pre), Some(post)) => Some(pre.saturating_sub(post)),
                    _ => None,
                };
                let quote_delta_raw = match (pre_quote, post_quote) {
                    (Some(pre), Some(post)) => Some(post as i128 - pre as i128),
                    _ => None,
                };
                let buy_range = find_instruction_range(&plan.instructions, &plan.buy_plan.instructions);
                let sell_range = find_instruction_range(&plan.instructions, &plan.sell_plan.instructions);
                let quote_flows = collect_token_flows(&quote_key);
                let base_flows = collect_token_flows(&base_key);
                let buy_quote_flow = buy_range.as_ref().map(|r| sum_flows(&quote_flows, r));
                let sell_quote_flow = sell_range.as_ref().map(|r| sum_flows(&quote_flows, r));
                let buy_base_flow = buy_range.as_ref().map(|r| sum_flows(&base_flows, r));
                let sell_base_flow = sell_range.as_ref().map(|r| sum_flows(&base_flows, r));
                let buy_quote_spent = buy_quote_flow.map(|f| f.out as i128 - f.in_ as i128);
                let sell_quote_received = sell_quote_flow.map(|f| f.in_ as i128 - f.out as i128);
                let buy_base_received = buy_base_flow.map(|f| f.in_ as i128 - f.out as i128);
                let sell_base_spent = sell_base_flow.map(|f| f.out as i128 - f.in_ as i128);

                let base_decimals = plan.buy_base_decimals;
                let quote_decimals = plan.buy_quote_decimals;

                let buy_quote_spent_ui = amount_to_ui(buy_quote_spent, quote_decimals);
                let sell_quote_received_ui = amount_to_ui(sell_quote_received, quote_decimals);
                let buy_base_received_ui = amount_to_ui(buy_base_received, base_decimals);
                let sell_base_spent_ui = amount_to_ui(sell_base_spent, base_decimals);

                let base_received_ui = match (base_received, base_decimals) {
                    (Some(amount), Some(d)) => {
                        let scale = 10f64.powi(d as i32);
                        if scale.is_finite() && scale > 0.0 {
                            Some(amount as f64 / scale)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                let quote_spent_ui = match (quote_spent, quote_decimals) {
                    (Some(amount), Some(d)) => {
                        let scale = 10f64.powi(d as i32);
                        if scale.is_finite() && scale > 0.0 {
                            Some(amount as f64 / scale)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };

                let expected_base_out = plan.buy_expected_base_out.or_else(|| {
                    if let (Some(quote_in), Some(price)) =
                        (plan.buy_expected_quote_in, plan.buy_expected_price)
                    {
                        if price > 0.0 {
                            Some(quote_in / price)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                });

                let effective_price = match (quote_spent_ui, base_received_ui) {
                    (Some(q), Some(b)) if b > 0.0 => Some(q / b),
                    _ => None,
                };
                let buy_leg_price = match (buy_quote_spent_ui, buy_base_received_ui) {
                    (Some(q), Some(b)) if b > 0.0 => Some(q / b),
                    _ => None,
                };
                let sell_leg_price = match (sell_quote_received_ui, sell_base_spent_ui) {
                    (Some(q), Some(b)) if b > 0.0 => Some(q / b),
                    _ => None,
                };

                let slippage_pct = match (expected_base_out, base_received_ui) {
                    (Some(expected), Some(actual)) if expected > 0.0 => {
                        Some((expected - actual) / expected * 100.0)
                    }
                    _ => None,
                };
                let slippage_bps = slippage_pct.map(|p| p * 100.0);
                let buy_leg_slippage_pct = match (expected_base_out, buy_base_received_ui) {
                    (Some(expected), Some(actual)) if expected > 0.0 => {
                        Some((expected - actual) / expected * 100.0)
                    }
                    _ => None,
                };
                let buy_leg_slippage_bps = buy_leg_slippage_pct.map(|p| p * 100.0);
                let sell_expected_quote_out = match (sell_base_spent_ui, plan.sell_expected_price) {
                    (Some(base_in), Some(price)) if price > 0.0 => Some(base_in * price),
                    _ => None,
                };
                let sell_leg_slippage_pct = match (sell_expected_quote_out, sell_quote_received_ui) {
                    (Some(expected), Some(actual)) if expected > 0.0 => {
                        Some((expected - actual) / expected * 100.0)
                    }
                    _ => None,
                };
                let sell_leg_slippage_bps = sell_leg_slippage_pct.map(|p| p * 100.0);

                sim_summary = Some(SimSwapSummary {
                    quote_spent_ui,
                    base_received_ui,
                    slippage_pct,
                    slippage_bps,
                });

                let base_mint_str = base_mint
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let quote_mint_str = quote_mint
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                let lines = vec![
                    format!(
                        "  base_account={} mint={} pre={} post={} received={}",
                        base_key,
                        base_mint_str,
                        format_amount(pre_base, base_decimals),
                        format_amount(post_base, base_decimals),
                        format_amount(base_received, base_decimals)
                    ),
                    format!(
                        "  quote_account={} mint={} pre={} post={} spent={}",
                        quote_key,
                        quote_mint_str,
                        format_amount(pre_quote, quote_decimals),
                        format_amount(post_quote, quote_decimals),
                        format_amount(quote_spent, quote_decimals)
                    ),
                    format!(
                        "  buy_leg_quote_spent={}",
                        format_signed_amount(buy_quote_spent, quote_decimals)
                    ),
                    format!(
                        "  sell_leg_quote_received={}",
                        format_signed_amount(sell_quote_received, quote_decimals)
                    ),
                    format!(
                        "  buy_leg_base_received={}",
                        format_signed_amount(buy_base_received, base_decimals)
                    ),
                    format!(
                        "  sell_leg_base_spent={}",
                        format_signed_amount(sell_base_spent, base_decimals)
                    ),
                    format!(
                        "  net_quote_delta={}",
                        format_signed_amount(quote_delta_raw, quote_decimals)
                    ),
                    format!(
                        "  expected: quote_in={} base_out={} price={}",
                        format_f64(plan.buy_expected_quote_in, 9),
                        format_f64(expected_base_out, 9),
                        format_f64(plan.buy_expected_price, 9)
                    ),
                    format!(
                        "  actual: quote_spent={} base_received={} price={} slippage={} ({} bps)",
                        format_f64(quote_spent_ui, 9),
                        format_f64(base_received_ui, 9),
                        format_f64(effective_price, 9),
                        format_f64(slippage_pct, 6),
                        format_f64(slippage_bps, 2)
                    ),
                    format!(
                        "  buy_leg: price={} expected_price={} slippage={} ({} bps)",
                        format_f64(buy_leg_price, 9),
                        format_f64(plan.buy_expected_price, 9),
                        format_f64(buy_leg_slippage_pct, 6),
                        format_f64(buy_leg_slippage_bps, 2)
                    ),
                    format!(
                        "  sell_leg: price={} expected_price={} slippage={} ({} bps)",
                        format_f64(sell_leg_price, 9),
                        format_f64(plan.sell_expected_price, 9),
                        format_f64(sell_leg_slippage_pct, 6),
                        format_f64(sell_leg_slippage_bps, 2)
                    ),
                ];

                if log_to_trade_log {
                    format!("SIM_SWAP:\n{}\n", lines.join("\n"))
                } else {
                    String::new()
                }
            }
        };

        if log_to_trade_log {
            let sim_block = format!(
                "\n========== TRADE #{} ✅ ==========\n\
                 STATUS: ✅ SIM_OK\n\
                 SLOT: {}\n\
                 UNITS_CONSUMED: {}\n\
                 RETURN_DATA: {}\n\
                 ACCOUNTS:\n{}\n\
                 SIM_LOGS:\n{}\n\
                 {}\
                 POOLS: buy={} sell={}\n\
                 BUY:  {}\n\
                 SELL: {}\n\
                 =====================================",
                trade_id,
                sim_slot,
                units,
                return_data,
                accounts_info,
                sim_logs,
                swap_summary,
                plan.buy_pool,
                plan.sell_pool,
                plan.buy_plan.description,
                plan.sell_plan.description,
            );
            append_trade_log(&sim_block);
        }

        Ok(sim_summary)
    }

    pub async fn preview_bundle(&self, plan: &AtomicTradePlan) -> Result<SubmissionSummary> {
        // ✅ 使用缓存的 blockhash，避免网络延迟
        let blockhash = self.ctx.get_blockhash().await
            .context("fetch blockhash for preview")?;
        let built = self.build_bundle(plan, blockhash)?;
        let signatures = extract_signatures(&built.transactions);
        info!(
            "Preview atomic bundle ({} tx). First entry: {}",
            built.base64.len(),
            built.base64.first().cloned().unwrap_or_default()
        );
        Ok(SubmissionSummary {
            bundle_base64: built.base64,
            signatures,
            relay: None,
            bundle_id: None,
        })
    }

    fn build_bundle(&self, plan: &AtomicTradePlan, blockhash: Hash) -> Result<BuiltBundle> {
        // ✅ 将 tip 指令嵌入到 atomic 交易中，实现真正的原子性
        // 只有 swap 成功，tip 才会执行
        let mut instructions = plan.instructions.clone();
        
        let tip_lamports = plan.tip_lamports;
        if tip_lamports > 0 {
            let receiver = self
                .tip
                .as_ref()
                .and_then(|tip| tip.receiver)
                .or_else(|| self.ctx.get_jito_tip_account());
            if let Some(receiver) = receiver {
                let tip_ix = build_tip_instruction(
                    &self.signer.pubkey(),
                    &receiver,
                    tip_lamports,
                );
                instructions.push(tip_ix);
                info!(
                    "Appended tip instruction: {} lamports to {}",
                    tip_lamports, receiver
                );
                append_trade_log(&format!(
                    "TIP appended lamports={} receiver={}",
                    tip_lamports, receiver
                ));
            } else if self.jito.is_some() || self.jito_grpc.is_some() {
                warn!("JITO relay configured but tip receiver is missing; bundle will have no tip");
            }
        }

        // 使用包含 tip 的 instructions 构建 atomic 交易
        let plan_with_tip = AtomicTradePlan {
            buy_plan: plan.buy_plan.clone(),
            sell_plan: plan.sell_plan.clone(),
            instructions,
            description: plan.description.clone(),
            net_quote_profit: plan.net_quote_profit,
            tip_lamports: plan.tip_lamports,
            buy_pool: plan.buy_pool,
            sell_pool: plan.sell_pool,
            buy_dex_kind: plan.buy_dex_kind,
            sell_dex_kind: plan.sell_dex_kind,
            buy_expected_quote_in: plan.buy_expected_quote_in,
            buy_expected_base_out: plan.buy_expected_base_out,
            buy_expected_price: plan.buy_expected_price,
            sell_expected_price: plan.sell_expected_price,
            buy_base_decimals: plan.buy_base_decimals,
            buy_quote_decimals: plan.buy_quote_decimals,
        };
        
        // ✅ 根据 DEX 类型选择需要的 ALT
        // Meteora (DLMM/LB/DAMM) → ALT #0
        // 其他 (Orca/Raydium/Pump.fun) → ALT #1
        let selected_tables = self.select_alts_for_plan(&plan_with_tip);
        debug!(
            "Selected {} ALT(s) for trade: buy={:?} sell={:?}",
            selected_tables.len(),
            plan.buy_dex_kind,
            plan.sell_dex_kind
        );
        
        let tx_ctx = AtomicTxContext::new(&*self.signer, blockhash, selected_tables);
        let atomic_tx = build_atomic_transaction(&plan_with_tip, &tx_ctx)?;

        // Bundle 只包含一笔交易（所有操作在同一笔交易中，真正原子性）
        let txs = vec![atomic_tx];

        let bundle = Bundle::from_transactions(&txs)?;
        Ok(BuiltBundle {
            transactions: txs,
            base64: bundle.as_base64_strings(),
        })
    }
}

fn is_expired_blockhash(err: &anyhow::Error) -> bool {
    let msg = format!("{err:?}").to_lowercase();
    msg.contains("expired blockhash")
}

/// 从环境变量获取签名者公钥（只加载公钥，不需要完整 keypair）
/// 
/// 用于 spot-only 模式下自动注入 DLMM 执行配置。
/// 如果 SOLANA_SIGNER_KEYPAIR 未配置或加载失败，返回 None。
pub fn get_signer_pubkey_from_env() -> Option<Pubkey> {
    use once_cell::sync::OnceCell;
    static CACHED: OnceCell<Option<Pubkey>> = OnceCell::new();
    
    *CACHED.get_or_init(|| {
        let path = std::env::var("SOLANA_SIGNER_KEYPAIR").ok()?;
        if path.trim().is_empty() {
            return None;
        }
        
        match read_keypair_file(path.trim()) {
            Ok(kp) => Some(kp.pubkey()),
            Err(err) => {
                log::warn!("Failed to load SOLANA_SIGNER_KEYPAIR for pubkey extraction: {}", err);
                None
            }
        }
    })
}

fn load_tip_settings() -> Result<Option<TipSettings>> {
    let receiver = match std::env::var("JITO_TIP_RECEIVER") {
        Ok(value) if !value.trim().is_empty() => Some(
            Pubkey::from_str(value.trim())
                .with_context(|| format!("parse pubkey from JITO_TIP_RECEIVER"))?,
        ),
        _ => None,
    };

    let lamports = match std::env::var("JITO_TIP_LAMPORTS") {
        Ok(value) => {
            let parsed: u64 = value
                .trim()
                .parse()
                .context("parse u64 from JITO_TIP_LAMPORTS")?;
            Some(parsed)
        }
        Err(_) => None,
    };

    let enabled = receiver.is_some() || lamports.unwrap_or(0) > 0;
    if enabled {
        Ok(Some(TipSettings { receiver }))
    } else {
        Ok(None)
    }
}

/// 刷新 Jito tip 账户缓存（用于自动选择 tip receiver）
pub async fn refresh_jito_tip_account(ctx: &AppContext) -> Result<()> {
    const TIP_REFRESH_TIMEOUT_SECS: u64 = 5;

    if let Ok(Some(client)) = JitoGrpcClient::from_env().await {
        match timeout(
            Duration::from_secs(TIP_REFRESH_TIMEOUT_SECS),
            client.get_tip_accounts(),
        )
        .await
        {
            Ok(Ok(accounts)) => return cache_tip_accounts(ctx, accounts).await,
            Ok(Err(err)) => warn!("[JITO] gRPC getTipAccounts failed: {err}"),
            Err(_) => warn!(
                "[JITO] gRPC getTipAccounts timed out after {}s",
                TIP_REFRESH_TIMEOUT_SECS
            ),
        }
    }

    let endpoint = match std::env::var("JITO_RELAY_URL") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return Ok(()),
    };

    let client = JitoClient::new(endpoint)?;
    let accounts = timeout(
        Duration::from_secs(TIP_REFRESH_TIMEOUT_SECS),
        client.get_tip_accounts(),
    )
    .await
    .map_err(|_| anyhow!("Jito getTipAccounts timed out after {}s", TIP_REFRESH_TIMEOUT_SECS))??;
    cache_tip_accounts(ctx, accounts).await
}

async fn cache_tip_accounts(ctx: &AppContext, accounts: Vec<String>) -> Result<()> {
    if accounts.is_empty() {
        warn!("[JITO] getTipAccounts returned empty list");
        return Ok(());
    }

    let mut parsed: Vec<Pubkey> = Vec::new();
    for account in accounts {
        match Pubkey::from_str(account.trim()) {
            Ok(key) => parsed.push(key),
            Err(err) => warn!("[JITO] invalid tip account: {err}"),
        }
    }

    if parsed.is_empty() {
        warn!("[JITO] No valid tip accounts parsed");
        return Ok(());
    }

    let idx = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as usize)
        % parsed.len();
    let selected = parsed[idx];
    ctx.set_jito_tip_account(Some(selected)).await;
    info!("[JITO] Cached tip account: {}", selected);
    Ok(())
}

async fn load_lookup_tables(ctx: &AppContext) -> Result<Vec<AddressLookupTableAccount>> {
    use solana_address_lookup_table_interface::program::ID as ADDRESS_LOOKUP_TABLE_PROGRAM_ID;

    let raw = std::env::var("JITO_LOOKUP_TABLES").unwrap_or_default();
    let keys: Vec<_> = raw
        .split(|c| matches!(c, ';' | ',' | '\n'))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();

    if keys.is_empty() {
        return Ok(Vec::new());
    }

    // ✅ 去重：避免重复加载同一个 ALT
    let mut seen: HashSet<Pubkey> = HashSet::new();
    let client = ctx.rpc_client();
    let mut tables = Vec::with_capacity(keys.len());

    for key_str in keys {
        // 容错处理：单个 ALT 加载失败不影响其他
        let key = match Pubkey::from_str(key_str) {
            Ok(k) => k,
            Err(err) => {
                warn!("Invalid lookup table pubkey '{}': {}", key_str, err);
                continue;
            }
        };

        // ✅ 跳过重复的 ALT
        if !seen.insert(key) {
            debug!("Skipping duplicate lookup table: {}", key);
            continue;
        }

        let account = match client.get_account(&key).await {
            Ok(acc) => acc,
            Err(err) => {
                warn!("Failed to fetch lookup table account {}: {:?}", key, err);
                continue;
            }
        };

        // ✅ 校验 owner 是否为 AddressLookupTable 程序
        if account.owner != ADDRESS_LOOKUP_TABLE_PROGRAM_ID {
            warn!(
                "Lookup table {} has invalid owner {} (expected {}), skipping",
                key, account.owner, ADDRESS_LOOKUP_TABLE_PROGRAM_ID
            );
            continue;
        }

        let table_state = match AddressLookupTable::deserialize(&account.data) {
            Ok(state) => state,
            Err(err) => {
                warn!("Failed to decode lookup table {}: {:?}", key, err);
                continue;
            }
        };

        // ✅ 校验 ALT 是否已停用
        if table_state.meta.deactivation_slot != u64::MAX {
            warn!(
                "Lookup table {} is deactivated (slot {}), skipping",
                key, table_state.meta.deactivation_slot
            );
            continue;
        }

        let addresses = table_state.addresses.into_owned();
        let address_count = addresses.len();
        tables.push(AddressLookupTableAccount { key, addresses });
        info!(
            "Loaded lookup table {} with {} addresses",
            key, address_count
        );
    }

    Ok(tables)
}

fn extract_signatures(txs: &[VersionedTransaction]) -> Vec<String> {
    txs.iter()
        .flat_map(|tx| tx.signatures.iter())
        .map(|sig| sig.to_string())
        .collect()
}

struct BuiltBundle {
    transactions: Vec<VersionedTransaction>,
    base64: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct SubmissionSummary {
    pub bundle_base64: Vec<String>,
    pub signatures: Vec<String>,
    pub relay: Option<String>,
    /// Jito bundle ID (用于追踪 bundle 状态)
    pub bundle_id: Option<String>,
}

#[derive(Clone)]
struct JitoClient {
    http: reqwest::Client,
    endpoint: String,
}

impl JitoClient {
    pub fn new(endpoint: String) -> Result<Self> {
        let http = reqwest::Client::builder().build()?;
        Ok(Self { http, endpoint })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn api_url(&self, path: &str) -> String {
        let trimmed = self.endpoint.trim_end_matches('/');
        if let Some(idx) = trimmed.find("/api/v1") {
            let base = &trimmed[..idx];
            return format!("{}{}", base, path);
        }
        format!("{}{}", trimmed, path)
    }

    /// 使用官方 Jito Block Engine JSON-RPC 格式提交 bundle
    /// 格式: {"jsonrpc":"2.0","id":1,"method":"sendBundle","params":[["base64_tx1","base64_tx2",...],{"encoding":"base64"}]}
    pub async fn send_bundle(&self, txs: &[String]) -> Result<String> {
        if txs.is_empty() {
            bail!("cannot submit empty bundle");
        }

        // Jito bundle 最多 5 笔交易
        if txs.len() > 5 {
            bail!("Jito bundle exceeds maximum of 5 transactions (got {})", txs.len());
        }

        let payload = JitoRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendBundle",
            params: SendBundleParams(txs, SendBundleEncoding { encoding: "base64" }),
        };

        let bundle_url = self.api_url("/api/v1/bundles");

        let resp = self
            .http
            .post(&bundle_url)
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("submit bundle to {}", bundle_url))?;

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            bail!(
                "Jito block engine {} rejected bundle (status {}): {}",
                bundle_url,
                status,
                body
            );
        }

        // 解析响应获取 bundle_id
        let rpc_resp: JitoRpcResponse = serde_json::from_str(&body)
            .with_context(|| format!("parse Jito response: {}", body))?;

        if let Some(error) = rpc_resp.error {
            bail!("Jito RPC error: code={} message={}", error.code, error.message);
        }

        // ✅ 如果 result 为空，说明 Jito 没有返回有效的 bundle_id
        let bundle_id = rpc_resp.result
            .filter(|id| !id.is_empty())
            .ok_or_else(|| anyhow!("Jito returned empty bundle_id (possibly rate limited or invalid bundle)"))?;
        Ok(bundle_id)
    }

    /// 获取可用的 Jito tip 账户列表
    pub async fn get_tip_accounts(&self) -> Result<Vec<String>> {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTipAccounts",
            "params": []
        });

        let tip_url = self.api_url("/api/v1/getTipAccounts");

        let resp = self
            .http
            .post(&tip_url)
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("fetch tip accounts from {}", tip_url))?;

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            bail!(
                "Jito block engine {} rejected getTipAccounts (status {}): {}",
                tip_url,
                status,
                body
            );
        }

        let rpc_resp: JitoTipAccountsResponse = serde_json::from_str(&body)
            .with_context(|| format!("parse Jito tip accounts response: {}", body))?;

        if let Some(error) = rpc_resp.error {
            bail!("Jito RPC error: code={} message={}", error.code, error.message);
        }

        Ok(rpc_resp.result.unwrap_or_default())
    }
}

const DLMM_SWAP_DISCRIMINATOR: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

#[derive(Debug)]
struct DlmmSwapInfo {
    pool: Pubkey,
    user: Pubkey,
    user_token_in: Pubkey,
    user_token_out: Pubkey,
    amount_in: u64,
    min_amount_out: u64,
}

fn parse_dlmm_swap_ix(ix: &Instruction) -> Result<DlmmSwapInfo> {
    if ix.program_id != METEORA_LB_PROGRAM_ID {
        bail!("not Meteora DLMM program");
    }
    if ix.data.len() != 24 {
        bail!("unexpected data len {}", ix.data.len());
    }
    if ix.data[..8] != DLMM_SWAP_DISCRIMINATOR {
        bail!("swap discriminator mismatch");
    }
    if ix.accounts.len() < 11 {
        bail!("not enough accounts (need >= 11, got {})", ix.accounts.len());
    }

    let amount_in = u64::from_le_bytes(ix.data[8..16].try_into().unwrap());
    let min_amount_out = u64::from_le_bytes(ix.data[16..24].try_into().unwrap());

    Ok(DlmmSwapInfo {
        pool: ix.accounts[0].pubkey,
        user_token_in: ix.accounts[4].pubkey,
        user_token_out: ix.accounts[5].pubkey,
        user: ix.accounts[10].pubkey,
        amount_in,
        min_amount_out,
    })
}

fn dlmm_remaining_accounts(ix: &Instruction) -> Option<Vec<Pubkey>> {
    let event_authority =
        Pubkey::find_program_address(&[b"__event_authority"], &METEORA_LB_PROGRAM_ID).0;

    if let Some(event_idx) = ix
        .accounts
        .iter()
        .position(|meta| meta.pubkey == event_authority)
    {
        let program_idx = event_idx + 1;
        if let Some(program_meta) = ix.accounts.get(program_idx) {
            if program_meta.pubkey == METEORA_LB_PROGRAM_ID {
                return Some(
                    ix.accounts
                        .iter()
                        .skip(program_idx + 1)
                        .map(|m| m.pubkey)
                        .collect(),
                );
            }
        }
    }

    // Fallback: take the last occurrence of program id (host_fee_in can also be program id).
    if let Some(idx) = ix
        .accounts
        .iter()
        .rposition(|meta| meta.pubkey == METEORA_LB_PROGRAM_ID)
    {
        return Some(ix.accounts.iter().skip(idx + 1).map(|m| m.pubkey).collect());
    }

    None
}

fn format_pubkeys(keys: &[Pubkey], max: usize) -> String {
    if keys.is_empty() {
        return "<none>".to_string();
    }
    let shown: Vec<String> = keys.iter().take(max).map(|k| k.to_string()).collect();
    if keys.len() > max {
        format!("[{} ... +{}]", shown.join(", "), keys.len() - max)
    } else {
        format!("[{}]", shown.join(", "))
    }
}

async fn build_dlmm_swap_ix_via_sidecar(
    client: &SidecarClient,
    info: &DlmmSwapInfo,
    side: TradeSide,
) -> Result<Instruction> {
    let side = match side {
        TradeSide::Sell => Side::InBase,
        TradeSide::Buy => Side::InQuote,
    };

    client
        .build_meteora_dlmm_swap_ix(
            &info.pool.to_string(),
            side,
            info.amount_in,
            info.min_amount_out,
            &info.user,
            Some(&info.user_token_in),
            Some(&info.user_token_out),
        )
        .await
}

async fn collect_dlmm_leg_debug(
    label: &str,
    side: TradeSide,
    plan: &TradePlan,
    client: Option<&SidecarClient>,
    lines: &mut Vec<String>,
) {
    let mut found = false;
    for (idx, ix) in plan.instructions.iter().enumerate() {
        if ix.program_id != METEORA_LB_PROGRAM_ID {
            continue;
        }
        found = true;

        let info = match parse_dlmm_swap_ix(ix) {
            Ok(info) => info,
            Err(err) => {
                lines.push(format!("{label}#{idx} local_parse_error={err}"));
                continue;
            }
        };

        match dlmm_remaining_accounts(ix) {
            Some(bins) => lines.push(format!(
                "{label}#{idx} local_bin_arrays={} {}",
                bins.len(),
                format_pubkeys(&bins, 20)
            )),
            None => lines.push(format!("{label}#{idx} local_bin_arrays=unknown")),
        }

        match client {
            Some(client) => match build_dlmm_swap_ix_via_sidecar(client, &info, side).await {
                Ok(sdk_ix) => match dlmm_remaining_accounts(&sdk_ix) {
                    Some(bins) => lines.push(format!(
                        "{label}#{idx} sdk_bin_arrays={} {}",
                        bins.len(),
                        format_pubkeys(&bins, 20)
                    )),
                    None => lines.push(format!("{label}#{idx} sdk_bin_arrays=unknown")),
                },
                Err(err) => lines.push(format!("{label}#{idx} sdk_error={err}")),
            },
            None => lines.push(format!("{label}#{idx} sdk_error=sidecar_unavailable")),
        }
    }

    if !found {
        lines.push(format!("{label} no DLMM swap ix found"));
    }
}

async fn collect_dlmm_bin_debug(plan: &AtomicTradePlan) -> String {
    let mut lines = Vec::new();
    let sidecar = match SidecarClient::from_env() {
        Ok(client) => Some(client),
        Err(err) => {
            lines.push(format!("sidecar_init_error={err}"));
            None
        }
    };

    if plan.buy_dex_kind == DexKind::MeteoraDlmm {
        collect_dlmm_leg_debug(
            "BUY",
            TradeSide::Buy,
            &plan.buy_plan,
            sidecar.as_ref(),
            &mut lines,
        )
        .await;
    }

    if plan.sell_dex_kind == DexKind::MeteoraDlmm {
        collect_dlmm_leg_debug(
            "SELL",
            TradeSide::Sell,
            &plan.sell_plan,
            sidecar.as_ref(),
            &mut lines,
        )
        .await;
    }

    if lines.is_empty() {
        "<none>".to_string()
    } else {
        lines.join("\n")
    }
}

#[derive(Serialize)]
struct JitoRpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: SendBundleParams<'a>,
}

#[derive(Serialize)]
struct SendBundleParams<'a>(&'a [String], SendBundleEncoding);

#[derive(Serialize)]
struct SendBundleEncoding {
    encoding: &'static str,
}

#[derive(Deserialize)]
struct JitoRpcResponse {
    result: Option<String>,
    error: Option<JitoRpcError>,
}

#[derive(Deserialize)]
struct JitoTipAccountsResponse {
    result: Option<Vec<String>>,
    error: Option<JitoRpcError>,
}

#[derive(Deserialize)]
struct JitoRpcError {
    code: i64,
    message: String,
}
