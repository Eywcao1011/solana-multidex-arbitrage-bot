//! Raydium AMM V4 trade executor - fully local
//! 
//! Both Quote (simulate_trade) and Build IX are local.
//! Serum accounts are obtained from sidecar introspect and cached.

use anyhow::{anyhow, bail, Result};
use log::debug;
use solana_instruction::{AccountMeta, Instruction};
use solana_pubkey::Pubkey;

use super::{TradeExecutor, TradePlan, TradeRequest};
use crate::dex::{raydium_amm, DexKind, PoolSnapshot, TradeSide};

const DEFAULT_RAYDIUM_AMM_SLIPPAGE_BPS: f64 = 50.0;

/// Raydium AMM V4 swap_base_in instruction index
const SWAP_BASE_IN: u8 = 9;

#[derive(Default)]
pub struct RaydiumAmmExecutor;

impl TradeExecutor for RaydiumAmmExecutor {
    fn dex(&self) -> DexKind {
        DexKind::RaydiumAmmV4
    }

    fn prepare_swap(&self, snapshot: &PoolSnapshot, request: &TradeRequest) -> Result<TradePlan> {
        // Basic validation
        // For exact-in Buy mode (quote_amount_in set), base_amount can be 0
        let is_exact_in_buy = request.quote_amount_in.is_some() && request.side == TradeSide::Buy;
        if !is_exact_in_buy && request.base_amount <= 0.0 {
            bail!("base_amount must be positive, got {}", request.base_amount);
        }

        if request.min_amount_out.is_none() {
            bail!("min_amount_out (protection) required for Raydium AMM execution");
        }

        if request.accounts.payer == Pubkey::default() {
            bail!("payer account must be set");
        }

        // Get cached static info (must have been populated by decode)
        let static_info = raydium_amm::get_static_info(&snapshot.descriptor.address)
            .ok_or_else(|| anyhow!("Raydium AMM static info not cached for pool {}", snapshot.descriptor.address))?;

        // Check if Serum accounts are available
        if static_info.serum_bids == Pubkey::default() {
            bail!("Serum accounts not available - sidecar needs to return serum_bids/asks/etc");
        }

        let base_decimals = snapshot.base_decimals;
        let quote_decimals = snapshot.quote_decimals;

        // ✅ 处理 inverted 映射：归一化的 base/quote 可能与池子的 base/quote 方向相反
        let inverted = snapshot.normalized_pair.inverted;
        let (norm_base_decimals, norm_quote_decimals) = if inverted {
            (quote_decimals, base_decimals)
        } else {
            (base_decimals, quote_decimals)
        };

        // For AMM V4:
        // - sell = base -> quote (swap_base_in)
        // - buy = quote -> base (swap_base_in with reversed tokens)
        let (amount_in, min_amount_out, user_source, user_destination) =
            match request.side {
                TradeSide::Sell => {
                    // Selling normalized base for normalized quote
                    let amount_in = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                    let min_out =
                        units_to_amount_raw(request.min_amount_out.unwrap(), norm_quote_decimals)?;
                    (
                        amount_in,
                        min_out,
                        request.accounts.user_base_account,
                        request.accounts.user_quote_account,
                    )
                }
                TradeSide::Buy => {
                    // Buying normalized base with normalized quote
                    let (amount_in, min_out) = if let Some(quote_in) = request.quote_amount_in {
                        // ✅ Spot-only exact-in mode: fixed quote input, min base output
                        let amount_in = units_to_amount_raw(quote_in, norm_quote_decimals)?;
                        let min_base_out = request.min_amount_out.unwrap_or(0.0);
                        let min_out =
                            units_to_amount_raw(min_base_out.max(0.000001), norm_base_decimals)?;
                        (amount_in, min_out)
                    } else {
                        // ✅ Full mode: min_amount_out is max_quote_in, base_amount is expected output
                        let amount_in =
                            units_to_amount_raw(request.min_amount_out.unwrap(), norm_quote_decimals)?;
                        let min_out = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                        (amount_in, min_out)
                    };
                    (
                        amount_in,
                        min_out,
                        request.accounts.user_quote_account,
                        request.accounts.user_base_account,
                    )
                }
            };

        // Build the swap instruction locally
        let instruction = build_swap_instruction(
            &static_info.program_id,
            &snapshot.descriptor.address,
            &static_info.open_orders,
            &static_info.target_orders,
            &static_info.base_vault,
            &static_info.quote_vault,
            &static_info.market_program_id,
            &static_info.market_id,
            &static_info.serum_bids,
            &static_info.serum_asks,
            &static_info.serum_event_queue,
            &static_info.serum_coin_vault,
            &static_info.serum_pc_vault,
            &static_info.serum_vault_signer,
            &request.accounts.payer,
            &user_source,
            &user_destination,
            amount_in,
            min_amount_out,
        )?;

        debug!(
            "Raydium AMM V4 local builder: pool={} amount_in={} min_out={}",
            snapshot.descriptor.address, amount_in, min_amount_out
        );

        let slippage_bps = if request.max_slippage_bps > 0.0 {
            request.max_slippage_bps
        } else {
            DEFAULT_RAYDIUM_AMM_SLIPPAGE_BPS
        };

        let side_display = match request.side {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        };

        let description = format!(
            "Raydium AMM V4 {} {} base {:.6} @ {:.2} bps slippage (min {} {:.6}) [local]",
            side_display,
            snapshot.descriptor.label,
            request.base_amount,
            slippage_bps,
            if request.side == TradeSide::Sell { "out" } else { "in" },
            request.min_amount_out.unwrap_or(0.0)
        );

        Ok(TradePlan {
            instructions: vec![instruction],
            description,
        })
    }
}

/// Build Raydium AMM V4 swap instruction locally
/// 
/// Account order (from Raydium AMM V4 program):
/// 0. token_program - SPL Token Program
/// 1. amm - AMM account (pool)
/// 2. amm_authority - AMM authority PDA
/// 3. amm_open_orders - AMM open orders account
/// 4. amm_target_orders - AMM target orders account
/// 5. pool_coin_token_account - Pool's source token vault
/// 6. pool_pc_token_account - Pool's destination token vault
/// 7. serum_program - Serum/OpenBook program
/// 8. serum_market - Serum market account
/// 9. serum_bids - Serum bids account
/// 10. serum_asks - Serum asks account
/// 11. serum_event_queue - Serum event queue
/// 12. serum_coin_vault - Serum coin vault
/// 13. serum_pc_vault - Serum PC vault
/// 14. serum_vault_signer - Serum vault signer PDA
/// 15. user_source_token_account - User's source token account
/// 16. user_destination_token_account - User's destination token account
/// 17. user_source_owner - User (signer)
#[allow(clippy::too_many_arguments)]
fn build_swap_instruction(
    program_id: &Pubkey,
    amm: &Pubkey,
    amm_open_orders: &Pubkey,
    amm_target_orders: &Pubkey,
    pool_source_vault: &Pubkey,
    pool_dest_vault: &Pubkey,
    serum_program: &Pubkey,
    serum_market: &Pubkey,
    serum_bids: &Pubkey,
    serum_asks: &Pubkey,
    serum_event_queue: &Pubkey,
    serum_coin_vault: &Pubkey,
    serum_pc_vault: &Pubkey,
    serum_vault_signer: &Pubkey,
    user: &Pubkey,
    user_source: &Pubkey,
    user_destination: &Pubkey,
    amount_in: u64,
    min_amount_out: u64,
) -> Result<Instruction> {
    // Derive AMM authority PDA
    // Seeds: [b"amm authority"]
    let (amm_authority, _) = Pubkey::find_program_address(
        &[
            &[97, 109, 109, 32, 97, 117, 116, 104, 111, 114, 105, 116, 121], // "amm authority"
        ],
        program_id,
    );

    // Build instruction data
    // Layout: instruction_index(1) + amount_in(8) + min_amount_out(8)
    let mut data = Vec::with_capacity(17);
    data.push(SWAP_BASE_IN);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());

    // Build account metas
    let accounts = vec![
        AccountMeta::new_readonly(spl_token::ID, false),         // token_program
        AccountMeta::new(*amm, false),                           // amm
        AccountMeta::new_readonly(amm_authority, false),         // amm_authority
        AccountMeta::new(*amm_open_orders, false),               // amm_open_orders
        AccountMeta::new(*amm_target_orders, false),             // amm_target_orders
        AccountMeta::new(*pool_source_vault, false),             // pool_coin_token_account
        AccountMeta::new(*pool_dest_vault, false),               // pool_pc_token_account
        AccountMeta::new_readonly(*serum_program, false),        // serum_program
        AccountMeta::new(*serum_market, false),                  // serum_market
        AccountMeta::new(*serum_bids, false),                    // serum_bids
        AccountMeta::new(*serum_asks, false),                    // serum_asks
        AccountMeta::new(*serum_event_queue, false),             // serum_event_queue
        AccountMeta::new(*serum_coin_vault, false),              // serum_coin_vault
        AccountMeta::new(*serum_pc_vault, false),                // serum_pc_vault
        AccountMeta::new_readonly(*serum_vault_signer, false),   // serum_vault_signer
        AccountMeta::new(*user_source, false),                   // user_source_token_account
        AccountMeta::new(*user_destination, false),              // user_destination_token_account
        AccountMeta::new_readonly(*user, true),                  // user_source_owner (signer)
    ];

    Ok(Instruction {
        program_id: *program_id,
        accounts,
        data,
    })
}

fn units_to_amount_raw(amount: f64, decimals: u8) -> Result<u64> {
    if !(amount.is_finite() && amount > 0.0) {
        bail!("amount must be positive and finite");
    }
    let scale = 10f64.powi(decimals as i32);
    let scaled = (amount * scale).floor();
    if !(scaled.is_finite() && scaled >= 0.0) {
        bail!("scaled amount invalid");
    }
    if scaled < 1.0 {
        return Ok(1);
    }
    if scaled > u64::MAX as f64 {
        bail!("amount exceeds u64 range");
    }
    Ok(scaled as u64)
}

