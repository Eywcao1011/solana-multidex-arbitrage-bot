use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use log::{debug, warn};
use solana_hash::Hash;
use solana_instruction::Instruction;
use solana_message::{v0::Message as V0Message, AddressLookupTableAccount, VersionedMessage};
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use solana_transaction::versioned::VersionedTransaction;
use solana_system_interface::instruction as system_instruction;

use super::arbitrage::AtomicTradePlan;
use std::collections::HashSet;

/// 构建 atomic swap 交易所需的上下文
pub struct AtomicTxContext<'a> {
    pub payer: &'a dyn Signer,
    pub recent_blockhash: Hash,
    pub lookup_tables: Vec<AddressLookupTableAccount>,
}

impl<'a> AtomicTxContext<'a> {
    pub fn new(
        payer: &'a dyn Signer,
        recent_blockhash: Hash,
        lookup_tables: Vec<AddressLookupTableAccount>,
    ) -> Self {
        Self {
            payer,
            recent_blockhash,
            lookup_tables,
        }
    }
}

/// 根据 AtomicTradePlan 构建一笔 VersionedTransaction
pub fn build_atomic_transaction(
    plan: &AtomicTradePlan,
    ctx: &AtomicTxContext<'_>,
) -> Result<VersionedTransaction> {
    if plan.instructions.is_empty() {
        bail!("atomic trade plan contains no instructions");
    }

    let message = V0Message::try_compile(
        &ctx.payer.pubkey(),
        &plan.instructions,
        &ctx.lookup_tables,
        ctx.recent_blockhash,
    )?;

    let tx = VersionedTransaction::try_new(VersionedMessage::V0(message), &[ctx.payer])?;

    if log::log_enabled!(log::Level::Debug) {
        if let Ok(serialized) = bincode::serialize(&tx) {
            let tx_bytes = serialized.len();
            let static_keys = match &tx.message {
                VersionedMessage::V0(msg) => msg.account_keys.len(),
                _ => 0,
            };
            let (lookups_used, loaded_keys) = match &tx.message {
                VersionedMessage::V0(msg) => {
                    let loaded: usize = msg
                        .address_table_lookups
                        .iter()
                        .map(|lookup| lookup.writable_indexes.len() + lookup.readonly_indexes.len())
                        .sum();
                    (msg.address_table_lookups.len(), loaded)
                }
                _ => (0, 0),
            };

            let mut unique_keys: HashSet<Pubkey> = HashSet::new();
            for ix in &plan.instructions {
                unique_keys.insert(ix.program_id);
                for meta in &ix.accounts {
                    unique_keys.insert(meta.pubkey);
                }
            }
            let unique_total = unique_keys.len();

            debug!(
                "Atomic tx stats: bytes={} static_keys={} loaded_keys={} lookups_used={} total_keys={} unique_ix_keys={} ixs={} desc={} ",
                tx_bytes,
                static_keys,
                loaded_keys,
                lookups_used,
                static_keys + loaded_keys,
                unique_total,
                plan.instructions.len(),
                plan.description
            );

            if tx_bytes > 1232 {
                let mut lut_keys: HashSet<Pubkey> = HashSet::new();
                for table in &ctx.lookup_tables {
                    lut_keys.extend(table.addresses.iter().copied());
                }

                let (signer_keys, static_non_signer_keys) = match &tx.message {
                    VersionedMessage::V0(msg) => {
                        let signer_count = msg.header.num_required_signatures as usize;
                        (
                            msg.account_keys
                                .iter()
                                .take(signer_count)
                                .copied()
                                .collect::<HashSet<_>>(),
                            msg.account_keys.iter().skip(signer_count).copied().collect::<Vec<_>>(),
                        )
                    }
                    _ => (HashSet::new(), Vec::new()),
                };

                let mut missing_from_lut = Vec::new();
                for key in static_non_signer_keys {
                    if signer_keys.contains(&key) {
                        continue;
                    }
                    if !lut_keys.contains(&key) {
                        missing_from_lut.push(key);
                    }
                }

                let preview: Vec<String> = missing_from_lut
                    .iter()
                    .take(20)
                    .map(|k| k.to_string())
                    .collect();

                warn!(
                    "Atomic tx exceeds size limit: bytes={} static_keys={} loaded_keys={} lookups_used={} missing_from_lut_count={} missing_from_lut_first20={:?} desc={}",
                    tx_bytes,
                    static_keys,
                    loaded_keys,
                    lookups_used,
                    missing_from_lut.len(),
                    preview,
                    plan.description
                );
            }
        }
    }

    Ok(tx)
}

/// Tip 交易配置（通常用于 Jito bundle）
#[derive(Clone, Copy)]
pub struct TipTxConfig<'a> {
    pub payer: &'a dyn Signer,
    pub tip_receiver: Pubkey,
    pub lamports: u64,
}

pub fn build_tip_transaction(
    config: &TipTxConfig<'_>,
    blockhash: Hash,
) -> Result<VersionedTransaction> {
    if config.lamports == 0 {
        bail!("tip lamports must be greater than zero");
    }

    let instruction = system_instruction::transfer(
        &config.payer.pubkey(),
        &config.tip_receiver,
        config.lamports,
    );

    let message = V0Message::try_compile(&config.payer.pubkey(), &[instruction], &[], blockhash)?;

    let tx = VersionedTransaction::try_new(VersionedMessage::V0(message), &[config.payer])?;
    Ok(tx)
}

/// 构建 tip 指令（用于嵌入到 atomic 交易内，实现真正的原子性）
/// 
/// 把 tip 作为 atomic tx 的最后一条指令，确保只有 swap 成功才支付 tip
pub fn build_tip_instruction(payer: &Pubkey, tip_receiver: &Pubkey, lamports: u64) -> Instruction {
    system_instruction::transfer(payer, tip_receiver, lamports)
}

/// 可直接提交给 Jito API 的 bundle 封装
#[derive(Debug, Clone)]
pub struct Bundle {
    serialized: Vec<Vec<u8>>,
}

impl Bundle {
    pub fn from_transactions(txs: &[VersionedTransaction]) -> Result<Self> {
        if txs.is_empty() {
            bail!("bundle must contain at least one transaction");
        }
        let serialized = txs
            .iter()
            .map(|tx| {
                bincode::serialize(tx)
                    .map_err(|err| anyhow!("failed to serialize transaction for bundle: {err}"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { serialized })
    }

    pub fn len(&self) -> usize {
        self.serialized.len()
    }

    pub fn as_base64_strings(&self) -> Vec<String> {
        self.serialized
            .iter()
            .map(|bytes| BASE64_STANDARD.encode(bytes))
            .collect()
    }

    pub fn raw(&self) -> &[Vec<u8>] {
        &self.serialized
    }
}

