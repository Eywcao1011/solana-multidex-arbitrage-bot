use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use once_cell::sync::OnceCell;
use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_client::rpc_config::{
    RpcSimulateTransactionAccountsConfig, RpcSimulateTransactionConfig,
};
use solana_client::rpc_response::{Response, RpcSimulateTransactionResult};
use solana_instruction::{AccountMeta, Instruction};
use solana_message::Message;
use solana_pubkey::Pubkey;
use solana_keypair::{read_keypair_file, Keypair};
use solana_signer::Signer;
use solana_transaction::Transaction;
use spl_token::solana_program::program_pack::Pack;
use spl_associated_token_account::get_associated_token_address;
use spl_token::state::Account as SplTokenAccount;
use tokio::task;

use crate::config::dlmm::{DlmmConfig, DlmmPoolConfig};
use crate::dex::{meteora_dlmm, meteora_lb, DexKind, PoolSnapshot, TradeSide};
use crate::{constants::METEORA_LB_PROGRAM_ID, rpc::AppContext};

static SERVICE: OnceCell<CalibrationService> = OnceCell::new();

pub fn schedule(snapshot: PoolSnapshot, ctx: AppContext) {
    let service = SERVICE.get_or_init(CalibrationService::load);
    service.schedule(snapshot, ctx);
}

struct CalibrationService {
    dlmm: Option<Arc<DlmmCalibrator>>,
}

impl CalibrationService {
    fn load() -> Self {
        let dlmm = match DlmmCalibrator::from_env() {
            Ok(Some(calibrator)) => Some(Arc::new(calibrator)),
            Ok(None) => None,
            Err(err) => {
                log::warn!("DLMM calibration disabled: {err:?}");
                None
            }
        };
        Self { dlmm }
    }

    fn schedule(&self, snapshot: PoolSnapshot, ctx: AppContext) {
        if let Some(dlmm) = &self.dlmm {
            if snapshot.descriptor.kind == DexKind::MeteoraDlmm {
                let calibrator = Arc::clone(dlmm);
                task::spawn(async move {
                    if let Err(err) = calibrator.maybe_run(snapshot, ctx).await {
                        log::warn!("DLMM calibration failed: {err:?}");
                    }
                });
            }
        }
    }
}

struct DlmmCalibrator {
    signer: Arc<Keypair>,
    pools: HashMap<Pubkey, Arc<DlmmPoolConfig>>,
    state: DashMap<Pubkey, CalibrationState>,
    max_sim_per_min: u32,
    min_interval: Duration,
    default_slippage_bps: f64,
    event_authority: Pubkey,
}

struct CalibrationState {
    last_attempt: Option<Instant>,
    window_start: Instant,
    window_count: u32,
}

impl CalibrationState {
    fn new(now: Instant) -> Self {
        Self {
            last_attempt: None,
            window_start: now,
            window_count: 0,
        }
    }
}

impl DlmmCalibrator {
    fn from_env() -> Result<Option<Self>> {
        let path = match std::env::var("DLMM_CALIBRATION_CONFIG") {
            Ok(path) if !path.trim().is_empty() => path,
            _ => return Ok(None),
        };

        let config = DlmmConfig::load_from_path(Path::new(&path))?;

        if config.pools.is_empty() {
            log::warn!("DLMM calibration config contains no usable pools");
            return Ok(None);
        }

        let signer_path = config
            .signer_keypair
            .clone()
            .ok_or_else(|| anyhow!("DLMM calibration requires 'signer_keypair' in config"))?;
        let signer = Arc::new(
            read_keypair_file(&signer_path)
                .map_err(|err| anyhow!("load user keypair from {}: {err}", signer_path))?,
        );

        let max_sim_per_min = config.max_simulations_per_minute.unwrap_or(2).max(1);
        let min_interval = Duration::from_secs(config.min_interval_secs.unwrap_or(60));
        let default_slippage_bps = config.default_slippage_bps;
        let event_authority =
            Pubkey::find_program_address(&[b"__event_authority"], &METEORA_LB_PROGRAM_ID).0;

        let signer_pubkey = signer.pubkey();
        let mut pools = HashMap::new();
        for (pool, cfg) in config.pools {
            if cfg.user != signer_pubkey {
                return Err(anyhow!(
                    "DLMM calibration signer {} does not match pool user {} for pool {}",
                    signer_pubkey,
                    cfg.user,
                    pool
                ));
            }
            pools.insert(pool, Arc::new(cfg));
        }

        Ok(Some(Self {
            signer,
            pools,
            state: DashMap::new(),
            max_sim_per_min,
            min_interval,
            default_slippage_bps,
            event_authority,
        }))
    }

    fn ensure_user_atas(&self, pool_cfg: &DlmmPoolConfig) -> Result<()> {
        let expected_in = get_associated_token_address(&pool_cfg.user, &pool_cfg.token_x_mint);
        if expected_in != pool_cfg.user_token_in {
            return Err(anyhow!(
                "DLMM config user_token_in {} does not match derived ATA {}",
                pool_cfg.user_token_in,
                expected_in
            ));
        }

        let expected_out = get_associated_token_address(&pool_cfg.user, &pool_cfg.token_y_mint);
        if expected_out != pool_cfg.user_token_out {
            return Err(anyhow!(
                "DLMM config user_token_out {} does not match derived ATA {}",
                pool_cfg.user_token_out,
                expected_out
            ));
        }

        Ok(())
    }

    async fn maybe_run(&self, snapshot: PoolSnapshot, ctx: AppContext) -> Result<()> {
        let pool_cfg = match self.pools.get(&snapshot.descriptor.address) {
            Some(cfg) => Arc::clone(cfg),
            None => return Ok(()),
        };

        if !self.check_quota(&pool_cfg.pool) {
            return Ok(());
        }

        let result = self.run_calibration(pool_cfg, snapshot, ctx).await;
        if let Err(err) = &result {
            log::warn!("DLMM calibration error: {err:?}");
        }
        result
    }

    fn check_quota(&self, pool: &Pubkey) -> bool {
        let now = Instant::now();
        let mut state = self
            .state
            .entry(*pool)
            .or_insert_with(|| CalibrationState::new(now));

        if let Some(last) = state.last_attempt {
            if now.saturating_duration_since(last) < self.min_interval {
                return false;
            }
        }

        if now.saturating_duration_since(state.window_start) >= Duration::from_secs(60) {
            state.window_start = now;
            state.window_count = 0;
        }

        if state.window_count >= self.max_sim_per_min {
            return false;
        }

        state.window_count += 1;
        state.last_attempt = Some(now);
        true
    }

    async fn run_calibration(
        &self,
        pool_cfg: Arc<DlmmPoolConfig>,
        snapshot: PoolSnapshot,
        ctx: AppContext,
    ) -> Result<()> {
        let lb_state = snapshot
            .lb_state
            .as_ref()
            .ok_or_else(|| anyhow!("lb_state missing for Meteora DLMM"))?;
        if lb_state.bin_array_indexes.is_empty() {
            return Err(anyhow!("no bin array indexes available for calibration"));
        }

        if pool_cfg.trade_side != TradeSide::Sell {
            return Err(anyhow!("only trade_side=Sell currently supported"));
        }

        let amount_in = pool_cfg.amount_in;
        if amount_in == 0 {
            return Err(anyhow!("amount_in is zero"));
        }

        self.ensure_user_atas(&pool_cfg)?;

        let token_in_info = ctx.get_mint_info(&pool_cfg.token_x_mint).await?;
        let token_out_info = ctx.get_mint_info(&pool_cfg.token_y_mint).await?;

        let amount_in_units = amount_in as f64 / 10f64.powi(token_in_info.decimals as i32);
        if amount_in_units <= 0.0 {
            return Err(anyhow!("calibration amount too small after decimals"));
        }

        let quote = meteora_dlmm::simulate_trade(&snapshot, pool_cfg.trade_side, amount_in_units)
            .ok_or_else(|| anyhow!("local DLMM simulator returned None"))?;
        let expected_out = quote.quote_proceeds;
        if expected_out <= 0.0 {
            return Err(anyhow!("expected output is zero"));
        }

        let slippage = pool_cfg
            .slippage_bps
            .unwrap_or(self.default_slippage_bps)
            .max(0.1);
        let min_out_units = expected_out * (1.0 - slippage / 10_000.0);
        if min_out_units <= 0.0 {
            return Err(anyhow!("min_amount_out computed non-positive"));
        }

        let min_amount_out =
            (min_out_units * 10f64.powi(token_out_info.decimals as i32)).floor() as u64;
        if min_amount_out == 0 {
            return Err(anyhow!("min_amount_out rounded to zero"));
        }

        let instruction =
            self.build_instruction(&pool_cfg, &snapshot, lb_state, amount_in, min_amount_out)?;

        if pool_cfg.user != self.signer.pubkey() {
            return Err(anyhow!(
                "DLMM calibration signer {} does not match pool user {}",
                self.signer.pubkey(),
                pool_cfg.user
            ));
        }

        let message = Message::new(&[instruction], Some(&pool_cfg.user));
        let recent_blockhash = ctx
            .rpc_client()
            .get_latest_blockhash()
            .await
            .context("fetch recent blockhash for DLMM simulation")?;
        let mut tx = Transaction::new_unsigned(message);
        tx.try_sign(&[&*self.signer], recent_blockhash)?;

        let pre_token_in = ctx
            .get_token_account_amount(&pool_cfg.user_token_in)
            .await?;
        let pre_token_out = ctx
            .get_token_account_amount(&pool_cfg.user_token_out)
            .await?;

        let accounts_cfg = RpcSimulateTransactionAccountsConfig {
            encoding: Some(UiAccountEncoding::Base64),
            addresses: vec![
                pool_cfg.user_token_in.to_string(),
                pool_cfg.user_token_out.to_string(),
            ],
        };

        let sim_config = RpcSimulateTransactionConfig {
            sig_verify: false,
            commitment: None,
            replace_recent_blockhash: true,
            accounts: Some(accounts_cfg),
            min_context_slot: None,
            inner_instructions: false,
            encoding: None,
        };

        let response: Response<RpcSimulateTransactionResult> = ctx
            .rpc_client()
            .simulate_transaction_with_config(&tx, sim_config)
            .await
            .context("simulate DLMM transaction")?;

        let result = response.value;
        let accounts = match result.accounts.as_ref() {
            Some(accounts) => accounts,
            None => {
                log::info!(
                    "DLMM calibration {}: simulation returned no account data",
                    pool_cfg.pool
                );
                return Ok(());
            }
        };

        if accounts.len() < 2 {
            log::info!(
                "DLMM calibration {}: simulation returned insufficient account data",
                pool_cfg.pool
            );
            return Ok(());
        }

        let post_token_in =
            decode_spl_token_amount(accounts.get(0).and_then(|account| account.as_ref()));
        let post_token_out =
            decode_spl_token_amount(accounts.get(1).and_then(|account| account.as_ref()));

        self.log_result(
            pool_cfg,
            &result,
            pre_token_in,
            pre_token_out,
            post_token_in,
            post_token_out,
            amount_in,
            expected_out,
            token_in_info.decimals,
            token_out_info.decimals,
        );

        Ok(())
    }

    fn build_instruction(
        &self,
        pool_cfg: &DlmmPoolConfig,
        snapshot: &PoolSnapshot,
        lb_state: &meteora_lb::LbState,
        amount_in: u64,
        min_amount_out: u64,
    ) -> Result<Instruction> {
        const SWAP_DISCRIMINATOR: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

        let mut data = Vec::with_capacity(8 + 16);
        data.extend_from_slice(&SWAP_DISCRIMINATOR);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());

        let bitmap_extension = pool_cfg.bin_array_bitmap_extension.unwrap_or_else(|| {
            meteora_lb::bitmap_extension_address(
                &METEORA_LB_PROGRAM_ID,
                &snapshot.descriptor.address,
            )
        });

        let mut metas = vec![
            AccountMeta::new(snapshot.descriptor.address, false),
            AccountMeta::new(bitmap_extension, false),
            AccountMeta::new(pool_cfg.reserve_x, false),
            AccountMeta::new(pool_cfg.reserve_y, false),
            AccountMeta::new(pool_cfg.user_token_in, false),
            AccountMeta::new(pool_cfg.user_token_out, false),
            AccountMeta::new_readonly(pool_cfg.token_x_mint, false),
            AccountMeta::new_readonly(pool_cfg.token_y_mint, false),
            AccountMeta::new(pool_cfg.oracle, false),
        ];

        if let Some(host_fee) = pool_cfg.host_fee_in {
            metas.push(AccountMeta::new(host_fee, false));
        }

        metas.push(AccountMeta::new_readonly(pool_cfg.user, true));
        metas.push(AccountMeta::new_readonly(pool_cfg.token_x_program, false));
        metas.push(AccountMeta::new_readonly(pool_cfg.token_y_program, false));
        metas.push(AccountMeta::new_readonly(self.event_authority, false));

        let mut remaining: Vec<Pubkey> = lb_state
            .bin_array_indexes
            .iter()
            .map(|index| {
                meteora_lb::bin_array_address(
                    &METEORA_LB_PROGRAM_ID,
                    &snapshot.descriptor.address,
                    *index,
                )
            })
            .collect();
        remaining.extend(pool_cfg.additional_remaining_accounts.iter().copied());

        let mut seen: HashSet<Pubkey> = metas.iter().map(|meta| meta.pubkey).collect();
        for key in remaining {
            if seen.insert(key) {
                metas.push(AccountMeta::new(key, false));
            }
        }

        Ok(Instruction {
            program_id: METEORA_LB_PROGRAM_ID,
            accounts: metas,
            data,
        })
    }

    fn log_result(
        &self,
        pool_cfg: Arc<DlmmPoolConfig>,
        result: &RpcSimulateTransactionResult,
        pre_in: u64,
        pre_out: u64,
        post_in: Option<u64>,
        post_out: Option<u64>,
        amount_in_raw: u64,
        expected_out: f64,
        in_decimals: u8,
        out_decimals: u8,
    ) {
        if let Some(err) = result.err.as_ref() {
            log::warn!("DLMM calibration simulation returned error: {:?}", err);
            return;
        }

        let Some(post_in) = post_in else {
            log::info!(
                "DLMM calibration {}: missing simulated token_in account",
                pool_cfg.pool
            );
            return;
        };
        let Some(post_out) = post_out else {
            log::info!(
                "DLMM calibration {}: missing simulated token_out account",
                pool_cfg.pool
            );
            return;
        };

        let spent_in = pre_in.saturating_sub(post_in);
        let gained_out = post_out.saturating_sub(pre_out);

        let spent_in_units = spent_in as f64 / 10f64.powi(in_decimals as i32);
        let expected_in_units = amount_in_raw as f64 / 10f64.powi(in_decimals as i32);
        let gained_out_units = gained_out as f64 / 10f64.powi(out_decimals as i32);
        let diff_pct = if expected_out > 0.0 {
            (gained_out_units - expected_out) / expected_out * 100.0
        } else {
            0.0
        };

        log::info!(
            "DLMM calibration {} -> spent {:.6} (expected {:.6}) | received {:.6} (expected {:.6}) | diff {:.4}%",
            pool_cfg.pool,
            spent_in_units,
            expected_in_units,
            gained_out_units,
            expected_out,
            diff_pct
        );
    }
}

fn decode_spl_token_amount(account: Option<&UiAccount>) -> Option<u64> {
    let ui_account = account?;
    if ui_account.owner != spl_token::ID.to_string() {
        return None;
    }
    let data = ui_account.data.decode()?;
    let token = SplTokenAccount::unpack_from_slice(&data).ok()?;
    Some(token.amount)
}
