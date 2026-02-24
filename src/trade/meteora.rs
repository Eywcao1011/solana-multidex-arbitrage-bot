use anyhow::{bail, Result};
use solana_instruction::{AccountMeta, Instruction};
use solana_pubkey::Pubkey;

use super::{TradeExecutor, TradePlan, TradeRequest};
use crate::constants::METEORA_LB_PROGRAM_ID;
use crate::dex::{meteora_lb, DexKind, PoolSnapshot, TradeSide};

const DEFAULT_LB_SLIPPAGE_BPS: f64 = 50.0;

// Anchor discriminator for "swap" instruction
const SWAP_DISCRIMINATOR: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

#[derive(Default)]
pub struct MeteoraLbExecutor;

impl TradeExecutor for MeteoraLbExecutor {
    fn dex(&self) -> DexKind {
        DexKind::MeteoraLb
    }

    fn prepare_swap(&self, snapshot: &PoolSnapshot, request: &TradeRequest) -> Result<TradePlan> {
        let lb_accounts = snapshot
            .lb_accounts
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing lb_accounts for Meteora LB pool"))?;

        let lb_state = snapshot
            .lb_state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing lb_state for Meteora LB pool"))?;

        let _fees = snapshot
            .fees
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing fees for Meteora LB pool"))?;

        // For exact-in Buy mode (quote_amount_in set), base_amount can be 0
        let is_exact_in_buy = request.quote_amount_in.is_some() && request.side == TradeSide::Buy;
        if !is_exact_in_buy && request.base_amount <= 0.0 {
            bail!("base_amount must be positive, got {}", request.base_amount);
        }

        if request.accounts.payer == Pubkey::default() {
            bail!("payer account must be set");
        }

        let inverted = snapshot.normalized_pair.inverted;

        // Meteora LB uses X/Y semantics where X is the base token and Y is the quote token
        // Similar to token A/B in other DEXes
        let _swap_for_y = (request.side == TradeSide::Sell) ^ inverted;

        // Adjust decimals based on inverted flag
        let norm_base_decimals = if inverted {
            snapshot.quote_decimals
        } else {
            snapshot.base_decimals
        };
        let norm_quote_decimals = if inverted {
            snapshot.base_decimals
        } else {
            snapshot.quote_decimals
        };

        // Check if we have pre-calculated protection values or need to simulate locally
        let use_exact_in = request.quote_amount_in.is_some() && request.side == TradeSide::Buy;

        let (amount_in, min_amount_out) = if use_exact_in {
            // ✅ Spot-only exact-in mode for Buy: use quote_amount_in directly
            let quote_in = request.quote_amount_in.unwrap();
            let amount_in = units_to_amount_raw(quote_in, norm_quote_decimals)?;
            let min_base_out = request.min_amount_out.unwrap_or(0.0);
            let min_amount_out = units_to_amount_raw(min_base_out.max(0.000001), norm_base_decimals)?;
            (amount_in, min_amount_out)
        } else if let Some(min_out) = request.min_amount_out {
            // ✅ Pre-calculated protection mode (from build_atomic_trade_plan)
            match request.side {
                TradeSide::Sell => {
                    let amount_in = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                    let min_amount_out = units_to_amount_raw(min_out, norm_quote_decimals)?;
                    (amount_in, min_amount_out)
                }
                TradeSide::Buy => {
                    // min_out is max_quote_in, base_amount is expected output
                    let amount_in = units_to_amount_raw(min_out, norm_quote_decimals)?;
                    let min_amount_out = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                    (amount_in, min_amount_out)
                }
            }
        } else {
            // ✅ Legacy mode: simulate trade locally
            let slippage_bps = if request.max_slippage_bps > 0.0 {
                request.max_slippage_bps
            } else {
                DEFAULT_LB_SLIPPAGE_BPS
            }
            .max(0.1);

            let quote = meteora_lb::simulate_trade(snapshot, request.side, request.base_amount)
                .ok_or_else(|| anyhow::anyhow!("failed to simulate trade for Meteora LB pool"))?;

            match request.side {
                TradeSide::Sell => {
                    let amount_in = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                    let min_quote_out =
                        compute_min_amount_out(quote.quote_proceeds, slippage_bps, norm_quote_decimals)?;
                    (amount_in, min_quote_out)
                }
                TradeSide::Buy => {
                    let amount_out = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                    let max_quote_in =
                        compute_max_amount_in(quote.quote_cost, slippage_bps, norm_quote_decimals)?;
                    (max_quote_in, amount_out)
                }
            }
        };

        // ✅ user_base/quote_account 已经是 normalized 语义（由 dispatcher 处理 inverted）
        // 这里只需要映射到 token_x/token_y (底层 token0/token1)
        // normalized base = user_base_account, normalized quote = user_quote_account
        // 当 inverted=true: normalized base 对应 token_y, normalized quote 对应 token_x
        // 当 inverted=false: normalized base 对应 token_x, normalized quote 对应 token_y
        let (user_token_x, user_token_y) = if inverted {
            // inverted: normalized_base=token_y, normalized_quote=token_x
            (request.accounts.user_quote_account, request.accounts.user_base_account)
        } else {
            // not inverted: normalized_base=token_x, normalized_quote=token_y
            (request.accounts.user_base_account, request.accounts.user_quote_account)
        };

        if user_token_x == Pubkey::default() || user_token_y == Pubkey::default() {
            bail!(
                "user token accounts must be set (got X={}, Y={})",
                user_token_x,
                user_token_y
            );
        }

        let pool_address = snapshot.descriptor.address;

        // Get bin array addresses from lb_state
        // For now, we use a simplified approach - in production you'd compute based on active_bin_id
        let bin_array_bitmap_extension = Pubkey::find_program_address(
            &[b"bitmap_extension", pool_address.as_ref()],
            &METEORA_LB_PROGRAM_ID,
        )
        .0;

        // Compute bin arrays based on active bin
        let bin_array_lower = compute_bin_array_address(&pool_address, lb_state._active_id, -1)?;
        let bin_array_upper = compute_bin_array_address(&pool_address, lb_state._active_id, 1)?;

        // Build account metas according to Meteora LB IDL
        let accounts = vec![
            AccountMeta::new(pool_address, false),    // 0. lb_pair
            AccountMeta::new(bin_array_lower, false), // 1. bin_array_bitmap_extension_lower
            AccountMeta::new(bin_array_upper, false), // 2. bin_array_bitmap_extension_upper
            AccountMeta::new(bin_array_bitmap_extension, false), // 3. bin_array_bitmap_extension
            AccountMeta::new(lb_accounts.reserve_x, false), // 4. reserve_x
            AccountMeta::new(lb_accounts.reserve_y, false), // 5. reserve_y
            AccountMeta::new(user_token_x, false),    // 6. user_token_x
            AccountMeta::new(user_token_y, false),    // 7. user_token_y
            AccountMeta::new_readonly(snapshot.base_mint, false), // 8. token_x_mint
            AccountMeta::new_readonly(snapshot.quote_mint, false), // 9. token_y_mint
            AccountMeta::new(lb_accounts.oracle, false), // 10. oracle
            AccountMeta::new_readonly(request.accounts.payer, true), // 11. user (signer)
            AccountMeta::new_readonly(spl_token::ID, false), // 12. token_x_program
            AccountMeta::new_readonly(spl_token::ID, false), // 13. token_y_program
            AccountMeta::new_readonly(solana_system_interface::program::ID, false), // 14. system_program
            AccountMeta::new_readonly(solana_sysvar::rent::ID, false), // 15. rent
            AccountMeta::new_readonly(spl_associated_token_account::ID, false), // 16. associated_token_program
        ];

        #[repr(C, packed)]
        struct SwapParams {
            discriminator: [u8; 8],
            amount_in: u64,
            min_amount_out: u64,
        }

        let swap_params = SwapParams {
            discriminator: SWAP_DISCRIMINATOR,
            amount_in,
            min_amount_out,
        };

        let data = unsafe {
            std::slice::from_raw_parts(
                &swap_params as *const _ as *const u8,
                std::mem::size_of::<SwapParams>(),
            )
            .to_vec()
        };

        let instruction = Instruction {
            program_id: METEORA_LB_PROGRAM_ID,
            accounts,
            data,
        };

        let side_str = match request.side {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        };

        let description = format!(
            "Meteora LB {} {} base {:.6} @ {:.2} bps slippage (exp {} {:.6} quote)",
            side_str,
            if request.side == TradeSide::Sell {
                "sell"
            } else {
                "buy"
            },
            request.base_amount,
            request.max_slippage_bps,
            amount_in as f64 / 10f64.powi(norm_quote_decimals as i32),
            min_amount_out as f64 / 10f64.powi(norm_base_decimals as i32),
        );

        Ok(TradePlan {
            instructions: vec![instruction],
            description,
        })
    }
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

fn compute_min_amount_out(expected_out: f64, slippage_bps: f64, decimals: u8) -> Result<u64> {
    let min_out_units = expected_out * (1.0 - slippage_bps / 10_000.0);
    if min_out_units <= 0.0 {
        bail!("min amount out non-positive");
    }
    units_to_amount_raw(min_out_units, decimals)
}

fn compute_max_amount_in(expected_in: f64, slippage_bps: f64, decimals: u8) -> Result<u64> {
    let max_in_units = expected_in * (1.0 + slippage_bps / 10_000.0);
    if max_in_units <= 0.0 {
        bail!("max amount in non-positive");
    }
    let scale = 10f64.powi(decimals as i32);
    let raw = (max_in_units * scale).ceil();
    if !(raw.is_finite() && raw > 0.0) {
        bail!("max amount in invalid after scaling");
    }
    if raw > u64::MAX as f64 {
        bail!("max amount exceeds u64 range");
    }
    Ok(raw as u64)
}

fn compute_bin_array_address(pool: &Pubkey, active_bin_id: i32, offset: i32) -> Result<Pubkey> {
    // Meteora LB bin array derivation (continuous index, floor for negatives)
    let base_index = meteora_lb::bin_array_index_for(active_bin_id);
    let index = base_index + offset as i64;
    Ok(meteora_lb::bin_array_address(&METEORA_LB_PROGRAM_ID, pool, index))
}

