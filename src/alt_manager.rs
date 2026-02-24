//! Address Lookup Table (ALT) 管理模块
//!
//! 在 warmup 阶段自动填充 ALT，确保所有静态账户都在 ALT 中以优化交易大小。
//! 支持多个 ALT，确保**同一池子的账户在同一个 ALT 中**。

use anyhow::{bail, Context, Result};
use log::{debug, info, warn};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_address_lookup_table_interface::instruction::extend_lookup_table;
use solana_address_lookup_table_interface::state::AddressLookupTable;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use solana_transaction::Transaction;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// 单个 ALT 的最大地址数
const MAX_ADDRESSES_PER_ALT: usize = 256;
/// 每次扩展 ALT 的最大地址数
const MAX_ADDRESSES_PER_TX: usize = 30;
/// 轮询起点（用于在空白 ALT 之间分配池子）
static NEXT_ALT_FOR_NEW_POOL: AtomicUsize = AtomicUsize::new(0);

/// 单个 ALT 的状态
#[derive(Debug)]
struct AltState {
    address: Pubkey,
    existing_addresses: HashSet<Pubkey>,
}

/// ALT 管理器 - 支持多个 ALT，按池子分组
pub struct AltManager {
    rpc: Arc<RpcClient>,
    authority: Arc<Keypair>,
    /// 所有 ALT（按配置顺序）
    alts: Vec<AltState>,
}

impl AltManager {
    /// 从环境变量加载 ALT 地址
    pub async fn from_env(rpc: Arc<RpcClient>, authority: Arc<Keypair>) -> Result<Self> {
        let mut alts = Vec::new();
        
        // 从 JITO_LOOKUP_TABLES 加载现有 ALT
        if let Ok(alt_str) = std::env::var("JITO_LOOKUP_TABLES") {
            for addr_str in alt_str.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                match addr_str.parse::<Pubkey>() {
                    Ok(addr) => {
                        // 尝试加载现有 ALT 的地址
                        let mut existing = HashSet::new();
                        match rpc.get_account(&addr).await {
                            Ok(account) => {
                                if let Ok(table) = AddressLookupTable::deserialize(&account.data) {
                                    for pk in table.addresses.iter() {
                                        existing.insert(*pk);
                                    }
                                    info!("Loaded ALT {} with {} existing addresses", addr, existing.len());
                                }
                            }
                            Err(e) => {
                                warn!("Failed to load ALT {}: {}", addr, e);
                            }
                        }
                        alts.push(AltState {
                            address: addr,
                            existing_addresses: existing,
                        });
                    }
                    Err(e) => {
                        warn!("Invalid ALT address '{}': {}", addr_str, e);
                    }
                }
            }
        }
        
        if alts.is_empty() {
            warn!("No ALTs configured in JITO_LOOKUP_TABLES");
        }
        
        Ok(Self { rpc, authority, alts })
    }

    /// 获取所有 ALT 地址
    pub fn alt_addresses(&self) -> Vec<Pubkey> {
        self.alts.iter().map(|alt| alt.address).collect()
    }

    /// 获取所有 ALT 中已有的地址总数
    pub fn total_addresses(&self) -> usize {
        self.alts.iter().map(|alt| alt.existing_addresses.len()).sum()
    }

    /// 检查地址是否已在任意 ALT 中
    pub fn contains(&self, addr: &Pubkey) -> bool {
        self.alts.iter().any(|alt| alt.existing_addresses.contains(addr))
    }

    /// 获取指定 ALT 的剩余空间
    fn available_space(&self, alt_index: usize) -> usize {
        if alt_index >= self.alts.len() {
            return 0;
        }
        MAX_ADDRESSES_PER_ALT.saturating_sub(self.alts[alt_index].existing_addresses.len())
    }

    /// 扩展指定 ALT，添加新地址
    async fn extend_alt(&mut self, alt_index: usize, addresses: &[Pubkey]) -> Result<usize> {
        if alt_index >= self.alts.len() {
            bail!("ALT index {} out of bounds", alt_index);
        }

        let alt_address = self.alts[alt_index].address;
        let existing = &self.alts[alt_index].existing_addresses;

        // 过滤掉已存在的地址
        let new_addresses: Vec<Pubkey> = addresses
            .iter()
            .filter(|addr| !existing.contains(addr))
            .copied()
            .collect();

        if new_addresses.is_empty() {
            return Ok(0);
        }

        let mut added_count = 0;

        for chunk in new_addresses.chunks(MAX_ADDRESSES_PER_TX) {
            let extend_ix = extend_lookup_table(
                alt_address,
                self.authority.pubkey(),
                Some(self.authority.pubkey()),
                chunk.to_vec(),
            );

            let blockhash = self
                .rpc
                .get_latest_blockhash()
                .await
                .context("get blockhash for ALT extension")?;

            let tx = Transaction::new_signed_with_payer(
                &[extend_ix],
                Some(&self.authority.pubkey()),
                &[&*self.authority],
                blockhash,
            );

            match self.rpc.send_and_confirm_transaction(&tx).await {
                Ok(sig) => {
                    debug!(
                        "Extended ALT #{} with {} addresses (sig: {})",
                        alt_index + 1,
                        chunk.len(),
                        sig
                    );
                    // 更新本地缓存
                    for addr in chunk {
                        self.alts[alt_index].existing_addresses.insert(*addr);
                    }
                    added_count += chunk.len();
                }
                Err(e) => {
                    warn!("Failed to extend ALT #{} with {} addresses: {}", alt_index + 1, chunk.len(), e);
                }
            }
        }

        Ok(added_count)
    }

    /// 按池子账户重叠优先分配 ALT，尽量让同一池子的账户保持在同一个 ALT 中。
    /// - 若该池子已有账户落在某个 ALT，优先追加到该 ALT。
    /// - 若没有重叠，则选择空间最多的 ALT。
    /// - 若无法在单个 ALT 中容纳新增账户，则跳过（避免拆分）。
    ///
    /// pool_accounts: Vec<(pool_label, dex_kind, Vec<accounts>)>
    pub async fn ensure_pool_accounts(
        &mut self,
        pool_accounts: &[(String, crate::DexKind, Vec<Pubkey>)],
    ) -> Result<()> {
        if self.alts.is_empty() {
            warn!("No ALTs configured, cannot add accounts");
            return Ok(());
        }

        // 检查是否启用了自动扩展
        let auto_extend = std::env::var("ALT_AUTO_EXTEND")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        if !auto_extend {
            info!("ALT_AUTO_EXTEND is not enabled, skipping ALT population");
            return Ok(());
        }

        info!(
            "Adding accounts for {} pools to {} ALT(s) by pool overlap...",
            pool_accounts.len(),
            self.alts.len()
        );

        // 分别给每个 ALT 添加对应 DEX 类型的通用静态账户
        // ALT #0 (index 0): Meteora DLMM/LB/DAMM
        // ALT #1 (index 1): Orca/Raydium/Pump.fun
        if self.alts.len() >= 1 {
            let meteora_common = collect_meteora_static_accounts();
            let meteora_new: Vec<Pubkey> = meteora_common
                .iter()
                .filter(|a| !self.contains(a))
                .copied()
                .collect();
            if !meteora_new.is_empty() {
                let added = self.extend_alt(0, &meteora_new).await?;
                if added > 0 {
                    info!("Added {} Meteora common accounts to ALT #1", added);
                }
            }
        }
        
        if self.alts.len() >= 2 {
            let other_common = collect_other_dex_static_accounts();
            let other_new: Vec<Pubkey> = other_common
                .iter()
                .filter(|a| !self.contains(a))
                .copied()
                .collect();
            if !other_new.is_empty() {
                let added = self.extend_alt(1, &other_new).await?;
                if added > 0 {
                    info!("Added {} Orca/Raydium/Pump common accounts to ALT #2", added);
                }
            }
        }

        // 按池子账户重叠优先分配 ALT（不再按 DEX 选择）
        let mut pools_per_alt: Vec<usize> = vec![0; self.alts.len()];
        let common_accounts: HashSet<Pubkey> = collect_meteora_static_accounts()
            .into_iter()
            .chain(collect_other_dex_static_accounts())
            .collect();
        
        for (label, dex_kind, accounts) in pool_accounts {
            // 过滤掉已存在于任意 ALT 的地址，避免全局重复
            let new_accounts: Vec<Pubkey> = accounts
                .iter()
                .filter(|a| !self.contains(a))
                .copied()
                .collect();
            
            if new_accounts.is_empty() {
                continue;
            }

            // 统计所有 ALT 的账户重叠情况，并筛选能容纳新增账户的 ALT
            let mut max_overlap_all = 0usize;
            let mut candidates: Vec<(usize, usize, usize)> = Vec::new(); // (alt_idx, overlap, space)
            for (i, alt) in self.alts.iter().enumerate() {
                let overlap = accounts
                    .iter()
                    .filter(|a| !common_accounts.contains(a) && alt.existing_addresses.contains(a))
                    .count();
                if overlap > max_overlap_all {
                    max_overlap_all = overlap;
                }

                let space = self.available_space(i);
                if space >= new_accounts.len() {
                    candidates.push((i, overlap, space));
                }
            }

            if candidates.is_empty() {
                warn!(
                    "Pool {} ({:?}) needs {} addresses but no ALT has enough space! (tried {} ALTs)",
                    label,
                    dex_kind,
                    new_accounts.len(),
                    self.alts.len()
                );
                continue;
            }

            let selected_alt = if max_overlap_all > 0 {
                let max_overlap = candidates
                    .iter()
                    .map(|(_, overlap, _)| *overlap)
                    .max()
                    .unwrap_or(0);

                if max_overlap == 0 {
                    info!(
                        "Pool {} ({:?}) already exists in another ALT but no ALT has enough space to keep it together (need {} addresses); falling back to any ALT with space",
                        label,
                        dex_kind,
                        new_accounts.len()
                    );
                }

                if max_overlap > 0 {
                    let mut best: Option<(usize, usize, usize)> = None;
                    for candidate in candidates
                        .iter()
                        .copied()
                        .filter(|(_, overlap, _)| *overlap == max_overlap)
                    {
                        match best {
                            None => best = Some(candidate),
                            Some((_, best_overlap, best_space)) => {
                                let (_, overlap, space) = candidate;
                                if overlap > best_overlap || (overlap == best_overlap && space > best_space) {
                                    best = Some(candidate);
                                }
                            }
                        }
                    }
                    best.map(|(alt_idx, _, _)| alt_idx).unwrap_or(0)
                } else {
                    pick_alt_by_space_and_round_robin(&candidates, &pools_per_alt)
                }
            } else {
                pick_alt_by_space_and_round_robin(&candidates, &pools_per_alt)
            };

            let added = self.extend_alt(selected_alt, &new_accounts).await?;
            if added > 0 {
                pools_per_alt[selected_alt] += 1;
                debug!(
                    "Added {} accounts for pool {} ({:?}) to ALT #{}",
                    added,
                    label,
                    dex_kind,
                    selected_alt + 1
                );
            }
        }

        let summaries: Vec<String> = self
            .alts
            .iter()
            .enumerate()
            .map(|(i, alt)| {
                format!(
                    "ALT #{} = {} addresses ({} pools)",
                    i + 1,
                    alt.existing_addresses.len(),
                    pools_per_alt[i]
                )
            })
            .collect();
        info!("ALT population complete: {}", summaries.join(", "));

        Ok(())
    }
}

fn pick_alt_by_space_and_round_robin(
    candidates: &[(usize, usize, usize)],
    pools_per_alt: &[usize],
) -> usize {
    let max_space = candidates
        .iter()
        .map(|(_, _, space)| *space)
        .max()
        .unwrap_or(0);
    let mut max_space_candidates: Vec<(usize, usize)> = candidates
        .iter()
        .copied()
        .filter(|(_, _, space)| *space == max_space)
        .map(|(alt_idx, _overlap, space)| (alt_idx, space))
        .collect();

    let min_pools = max_space_candidates
        .iter()
        .map(|(alt_idx, _)| pools_per_alt.get(*alt_idx).copied().unwrap_or(0))
        .min()
        .unwrap_or(0);
    max_space_candidates.retain(|(alt_idx, _)| {
        pools_per_alt.get(*alt_idx).copied().unwrap_or(0) == min_pools
    });

    max_space_candidates.sort_by_key(|(alt_idx, _)| *alt_idx);
    let start = NEXT_ALT_FOR_NEW_POOL.fetch_add(1, Ordering::Relaxed);
    let pick = if max_space_candidates.is_empty() {
        0
    } else {
        start % max_space_candidates.len()
    };
    max_space_candidates
        .get(pick)
        .map(|(alt_idx, _)| *alt_idx)
        .unwrap_or(0)
}

/// 收集 Meteora DEX 专用的静态账户（用于 ALT #1）
/// 只包含系统程序 + Meteora 程序 ID
pub fn collect_meteora_static_accounts() -> Vec<Pubkey> {
    use solana_system_interface::program as system_program;
    use crate::constants::{METEORA_LB_PROGRAM_ID, METEORA_DAMM_V2_PROGRAM_ID};

    vec![
        // 通用系统程序
        system_program::ID,
        spl_token::ID,
        solana_pubkey::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"), // Token-2022
        solana_pubkey::pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"), // ATA
        solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111"),
        // Native SOL mint
        solana_pubkey::pubkey!("So11111111111111111111111111111111111111112"),
        // Meteora 程序 ID (LB/DLMM 是同一个程序)
        METEORA_LB_PROGRAM_ID,
        // Meteora DAMM V2 程序
        METEORA_DAMM_V2_PROGRAM_ID,
    ]
}

/// 收集其他 DEX (Orca/Raydium/Pump.fun) 专用的静态账户（用于 ALT #2）
/// 只包含系统程序 + 对应 DEX 程序 ID
pub fn collect_other_dex_static_accounts() -> Vec<Pubkey> {
    use solana_system_interface::program as system_program;
    use crate::constants::{
        ORCA_WHIRLPOOL_PROGRAM_ID, PUMP_FUN_PROGRAM_ID,
        RAYDIUM_CLMM_PROGRAM_ID, RAYDIUM_CPMM_PROGRAM_ID,
    };

    vec![
        // 通用系统程序
        system_program::ID,
        spl_token::ID,
        solana_pubkey::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"), // Token-2022
        solana_pubkey::pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"), // ATA
        solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111"),
        // Native SOL mint
        solana_pubkey::pubkey!("So11111111111111111111111111111111111111112"),
        // Orca
        ORCA_WHIRLPOOL_PROGRAM_ID,
        // Pump.fun
        PUMP_FUN_PROGRAM_ID,
        // Raydium
        RAYDIUM_CLMM_PROGRAM_ID,
        RAYDIUM_CPMM_PROGRAM_ID,
        // Raydium AMM V4
        solana_pubkey::pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"),
        // Serum/OpenBook
        solana_pubkey::pubkey!("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX"),
    ]
}

/// 保留原函数用于向后兼容（返回所有 DEX 的账户）
#[allow(dead_code)]
pub fn collect_common_static_accounts() -> Vec<Pubkey> {
    let mut all = collect_meteora_static_accounts();
    all.extend(collect_other_dex_static_accounts());
    // 去重
    let mut seen = std::collections::HashSet::new();
    all.retain(|x| seen.insert(*x));
    all
}

