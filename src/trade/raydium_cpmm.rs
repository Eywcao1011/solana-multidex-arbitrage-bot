//! Raydium CPMM (cp-swap) trade executor - local builder

use anyhow::{anyhow, bail, Result};
use log::debug;
use solana_instruction::{AccountMeta, Instruction};
use solana_pubkey::Pubkey;

use super::{TradeExecutor, TradePlan, TradeRequest};
use crate::dex::{raydium_cpmm, DexKind, PoolSnapshot, TradeSide};

const DEFAULT_RAYDIUM_CPMM_SLIPPAGE_BPS: f64 = 50.0;

/// Anchor discriminator for `swap_base_input`
/// from Raydium IDL: raydium_cpmm/raydium_cp_swap.json
const SWAP_BASE_INPUT_DISCRIMINATOR: [u8; 8] = [143, 190, 90, 218, 196, 30, 51, 222];

/// Raydium cp-swap AUTH_SEED = "vault_and_lp_mint_auth_seed"
const AUTH_SEED: &[u8] = b"vault_and_lp_mint_auth_seed";

#[derive(Default)]
pub struct RaydiumCpmmExecutor;

impl TradeExecutor for RaydiumCpmmExecutor {
    fn dex(&self) -> DexKind {
        DexKind::RaydiumCpmm
    }

    fn prepare_swap(&self, snapshot: &PoolSnapshot, request: &TradeRequest) -> Result<TradePlan> {
        if request.accounts.payer == Pubkey::default() {
            bail!("payer account must be set");
        }
        if request.accounts.user_base_account == Pubkey::default() {
            bail!("user_base_account must be set");
        }
        if request.accounts.user_quote_account == Pubkey::default() {
            bail!("user_quote_account must be set");
        }

        // Static info must have been cached by decode
        let static_info = raydium_cpmm::get_static_info(&snapshot.descriptor.address).ok_or_else(|| {
            anyhow!(
                "Raydium CPMM static info not cached for pool {}",
                snapshot.descriptor.address
            )
        })?;

        // ✅ 处理 inverted 映射
        // snapshot.base_mint/quote_mint 是原始 token0/token1
        // 需要根据 inverted 获取 normalized mint
        let inverted = snapshot.normalized_pair.inverted;
        let (norm_base_mint, norm_quote_mint) = if inverted {
            (snapshot.quote_mint, snapshot.base_mint)
        } else {
            (snapshot.base_mint, snapshot.quote_mint)
        };
        let (norm_base_decimals, norm_quote_decimals) = if inverted {
            (snapshot.quote_decimals, snapshot.base_decimals)
        } else {
            (snapshot.base_decimals, snapshot.quote_decimals)
        };

        // CPMM uses normalized base/quote semantics in PoolSnapshot
        // - Sell: base -> quote
        // - Buy: quote -> base
        let use_exact_in_buy = request.quote_amount_in.is_some() && request.side == TradeSide::Buy;

        let (input_mint, output_mint, user_input, user_output, amount_in_units, min_out_units) =
            if use_exact_in_buy {
                // Spot-only exact-in buy: spend fixed quote_amount_in, receive at least min_base_out
                let quote_in = request
                    .quote_amount_in
                    .ok_or_else(|| anyhow!("quote_amount_in required for exact-in buy"))?;
                let min_base_out = request.min_amount_out.unwrap_or(0.0);

                (
                    norm_quote_mint,
                    norm_base_mint,
                    request.accounts.user_quote_account,
                    request.accounts.user_base_account,
                    units_to_amount_raw(quote_in, norm_quote_decimals)?,
                    units_to_amount_raw(min_base_out.max(0.000001), norm_base_decimals)?,
                )
            } else if let Some(min_out) = request.min_amount_out {
                // Pre-calculated protection mode (from build_atomic_trade_plan)
                match request.side {
                    TradeSide::Sell => (
                        norm_base_mint,
                        norm_quote_mint,
                        request.accounts.user_base_account,
                        request.accounts.user_quote_account,
                        units_to_amount_raw(request.base_amount, norm_base_decimals)?,
                        units_to_amount_raw(min_out, norm_quote_decimals)?,
                    ),
                    TradeSide::Buy => (
                        norm_quote_mint,
                        norm_base_mint,
                        request.accounts.user_quote_account,
                        request.accounts.user_base_account,
                        // For buy leg in full mode, min_amount_out is max_quote_in
                        units_to_amount_raw(min_out, norm_quote_decimals)?,
                        // base_amount is expected base out
                        units_to_amount_raw(request.base_amount, norm_base_decimals)?,
                    ),
                }
            } else {
                // Legacy mode: derive min_out from local simulation
                let slippage_bps = if request.max_slippage_bps > 0.0 {
                    request.max_slippage_bps
                } else {
                    DEFAULT_RAYDIUM_CPMM_SLIPPAGE_BPS
                }
                .max(0.1);

                let quote = crate::dex::simulate_trade(snapshot, request.side, request.base_amount)
                    .ok_or_else(|| anyhow!("failed to simulate trade for Raydium CPMM pool"))?;

                match request.side {
                    TradeSide::Sell => (
                        norm_base_mint,
                        norm_quote_mint,
                        request.accounts.user_base_account,
                        request.accounts.user_quote_account,
                        units_to_amount_raw(request.base_amount, norm_base_decimals)?,
                        compute_min_amount_out(quote.quote_proceeds, slippage_bps, norm_quote_decimals)?,
                    ),
                    TradeSide::Buy => (
                        norm_quote_mint,
                        norm_base_mint,
                        request.accounts.user_quote_account,
                        request.accounts.user_base_account,
                        compute_max_amount_in(quote.quote_cost, slippage_bps, norm_quote_decimals)?,
                        units_to_amount_raw(request.base_amount, norm_base_decimals)?,
                    ),
                }
            };

        let (input_vault, output_vault, input_token_program, output_token_program) =
            resolve_vaults_and_programs(&static_info, input_mint, output_mint)?;

        let ix = build_swap_base_input_ix(
            &static_info.program_id,
            &request.accounts.payer,
            &static_info.amm_config,
            &snapshot.descriptor.address,
            &user_input,
            &user_output,
            &input_vault,
            &output_vault,
            &input_token_program,
            &output_token_program,
            &input_mint,
            &output_mint,
            &static_info.observation_key,
            amount_in_units,
            min_out_units,
        );

        debug!(
            "Raydium CPMM builder: pool={} amount_in={} min_out={} in_mint={} out_mint={}",
            snapshot.descriptor.address, amount_in_units, min_out_units, input_mint, output_mint
        );

        let slippage_bps = if request.max_slippage_bps > 0.0 {
            request.max_slippage_bps
        } else {
            DEFAULT_RAYDIUM_CPMM_SLIPPAGE_BPS
        };

        let side_display = match request.side {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        };

        let description = format!(
            "Raydium CPMM {} {} base {:.6} @ {:.2} bps slippage (min_out {:.6})",
            side_display,
            snapshot.descriptor.label,
            request.base_amount,
            slippage_bps,
            request.min_amount_out.unwrap_or(0.0)
        );

        Ok(TradePlan {
            instructions: vec![ix],
            description,
        })
    }
}

fn resolve_vaults_and_programs(
    info: &raydium_cpmm::CpmmPoolStaticInfo,
    input_mint: Pubkey,
    output_mint: Pubkey,
) -> Result<(Pubkey, Pubkey, Pubkey, Pubkey)> {
    let (input_vault, input_token_program) = if input_mint == info.token_0_mint {
        (info.token_0_vault, info.token_0_program)
    } else if input_mint == info.token_1_mint {
        (info.token_1_vault, info.token_1_program)
    } else {
        bail!("input mint {} not found in CPMM pool", input_mint);
    };

    let (output_vault, output_token_program) = if output_mint == info.token_0_mint {
        (info.token_0_vault, info.token_0_program)
    } else if output_mint == info.token_1_mint {
        (info.token_1_vault, info.token_1_program)
    } else {
        bail!("output mint {} not found in CPMM pool", output_mint);
    };

    if input_vault == output_vault {
        bail!("input_vault equals output_vault (invalid swap mapping)");
    }

    Ok((
        input_vault,
        output_vault,
        input_token_program,
        output_token_program,
    ))
}

fn build_swap_base_input_ix(
    program_id: &Pubkey,
    payer: &Pubkey,
    amm_config: &Pubkey,
    pool_state: &Pubkey,
    input_token_account: &Pubkey,
    output_token_account: &Pubkey,
    input_vault: &Pubkey,
    output_vault: &Pubkey,
    input_token_program: &Pubkey,
    output_token_program: &Pubkey,
    input_token_mint: &Pubkey,
    output_token_mint: &Pubkey,
    observation_state: &Pubkey,
    amount_in: u64,
    minimum_amount_out: u64,
) -> Instruction {
    let (authority, _) = Pubkey::find_program_address(&[AUTH_SEED], program_id);

    let mut data = Vec::with_capacity(8 + 8 + 8);
    data.extend_from_slice(&SWAP_BASE_INPUT_DISCRIMINATOR);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&minimum_amount_out.to_le_bytes());

    let accounts = vec![
        AccountMeta::new_readonly(*payer, true),            // payer
        AccountMeta::new_readonly(authority, false),       // authority
        AccountMeta::new_readonly(*amm_config, false),     // amm_config
        AccountMeta::new(*pool_state, false),              // pool_state (mut)
        AccountMeta::new(*input_token_account, false),     // input_token_account (mut)
        AccountMeta::new(*output_token_account, false),    // output_token_account (mut)
        AccountMeta::new(*input_vault, false),             // input_vault (mut)
        AccountMeta::new(*output_vault, false),            // output_vault (mut)
        AccountMeta::new_readonly(*input_token_program, false),  // input_token_program
        AccountMeta::new_readonly(*output_token_program, false), // output_token_program
        AccountMeta::new_readonly(*input_token_mint, false),     // input_token_mint
        AccountMeta::new_readonly(*output_token_mint, false),    // output_token_mint
        AccountMeta::new(*observation_state, false),       // observation_state (mut)
    ];

    Instruction {
        program_id: *program_id,
        accounts,
        data,
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

fn compute_min_amount_out(amount: f64, slippage_bps: f64, decimals: u8) -> Result<u64> {
    if !(amount.is_finite() && amount > 0.0) {
        bail!("amount invalid");
    }
    let factor = (1.0 - slippage_bps / 10_000.0).max(0.0);
    units_to_amount_raw(amount * factor, decimals)
}

fn compute_max_amount_in(amount: f64, slippage_bps: f64, decimals: u8) -> Result<u64> {
    if !(amount.is_finite() && amount > 0.0) {
        bail!("amount invalid");
    }
    let factor = (1.0 + slippage_bps / 10_000.0).max(1.0);
    units_to_amount_raw(amount * factor, decimals)
}

