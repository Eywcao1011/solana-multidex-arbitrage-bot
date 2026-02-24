use anyhow::{bail, Result};
use solana_instruction::{AccountMeta, Instruction};
use solana_pubkey::Pubkey;

use super::{TradeExecutor, TradePlan, TradeRequest};
use crate::constants::{
    METEORA_DAMM_POOL_AUTHORITY, METEORA_DAMM_V1_PROGRAM_ID, METEORA_DAMM_V2_PROGRAM_ID,
};
use crate::dex::{meteora_damm, DexKind, PoolSnapshot, TradeSide};

const DEFAULT_DAMM_SLIPPAGE_BPS: f64 = 50.0;

// Anchor discriminator for "swap" instruction
const SWAP_DISCRIMINATOR: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

#[derive(Default)]
pub struct MeteoraDammExecutor;

impl TradeExecutor for MeteoraDammExecutor {
    fn dex(&self) -> DexKind {
        DexKind::MeteoraDammV1
    }

    fn prepare_swap(&self, snapshot: &PoolSnapshot, request: &TradeRequest) -> Result<TradePlan> {
        let damm_accounts = snapshot
            .damm_accounts
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing damm_accounts for Meteora DAMM pool"))?;

        // For exact-in Buy mode (quote_amount_in set), base_amount can be 0
        let is_exact_in_buy = request.quote_amount_in.is_some() && request.side == TradeSide::Buy;
        if !is_exact_in_buy && request.base_amount <= 0.0 {
            bail!("base_amount must be positive, got {}", request.base_amount);
        }

        if request.accounts.payer == Pubkey::default() {
            bail!("payer account must be set");
        }
        if request.accounts.user_base_account == Pubkey::default() {
            bail!("user_base_account must be set");
        }
        if request.accounts.user_quote_account == Pubkey::default() {
            bail!("user_quote_account must be set");
        }

        let inverted = snapshot.normalized_pair.inverted;

        // When inverted, normalized base=token_b and normalized quote=token_a
        // Adjust decimals accordingly for correct unit conversions
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

        // Check if we have pre-calculated protection values (spot_only mode)
        // or need to simulate locally (full mode)
        let use_exact_in = request.quote_amount_in.is_some() && request.side == TradeSide::Buy;

        // DAMM uses token A/B semantics (not normalized base/quote)
        // base_mint is always token A, quote_mint is always token B
        let (input_token_account, output_token_account, amount_in, minimum_amount_out) =
            if use_exact_in {
                // ✅ Spot-only exact-in mode for Buy: use quote_amount_in directly
                let quote_in = request.quote_amount_in.unwrap();
                let amount_in = units_to_amount_raw(quote_in, norm_quote_decimals)?;
                let min_base_out = request.min_amount_out.unwrap_or(0.0);
                let minimum_amount_out = units_to_amount_raw(min_base_out, norm_base_decimals)?;

                (
                    request.accounts.user_quote_account, // input = quote
                    request.accounts.user_base_account,  // output = base
                    amount_in,
                    minimum_amount_out,
                )
            } else if let Some(min_out) = request.min_amount_out {
                // ✅ Pre-calculated protection mode (from build_atomic_trade_plan)
                match request.side {
                    TradeSide::Sell => {
                        // Sell: input base_amount, output at least min_out quote
                        let amount_in = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                        let minimum_amount_out = units_to_amount_raw(min_out, norm_quote_decimals)?;
                        (
                            request.accounts.user_base_account,
                            request.accounts.user_quote_account,
                            amount_in,
                            minimum_amount_out,
                        )
                    }
                    TradeSide::Buy => {
                        // Buy (full mode): min_out is max_quote_in, base_amount is expected output
                        let amount_in = units_to_amount_raw(min_out, norm_quote_decimals)?; // max_quote_in
                        let minimum_amount_out = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                        (
                            request.accounts.user_quote_account,
                            request.accounts.user_base_account,
                            amount_in,
                            minimum_amount_out,
                        )
                    }
                }
            } else {
                // ✅ Legacy mode: simulate trade locally
                let slippage_bps = if request.max_slippage_bps > 0.0 {
                    request.max_slippage_bps
                } else {
                    DEFAULT_DAMM_SLIPPAGE_BPS
                }
                .max(0.1);

                let quote = meteora_damm::simulate_trade(snapshot, request.side, request.base_amount)
                    .ok_or_else(|| anyhow::anyhow!("failed to simulate trade for Meteora DAMM pool"))?;

                match (request.side, inverted) {
                    (TradeSide::Sell, false) | (TradeSide::Sell, true) => {
                        let amount_in = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                        let min_quote_out = compute_min_amount_out(
                            quote.quote_proceeds,
                            slippage_bps,
                            norm_quote_decimals,
                        )?;
                        (
                            request.accounts.user_base_account,
                            request.accounts.user_quote_account,
                            amount_in,
                            min_quote_out,
                        )
                    }
                    (TradeSide::Buy, false) | (TradeSide::Buy, true) => {
                        let amount_out = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                        let max_quote_in = compute_max_amount_in(
                            quote.quote_cost,
                            slippage_bps,
                            norm_quote_decimals,
                        )?;
                        (
                            request.accounts.user_quote_account,
                            request.accounts.user_base_account,
                            max_quote_in,
                            amount_out,
                        )
                    }
                }
            };

        let program_id = match snapshot.descriptor.kind {
            DexKind::MeteoraDammV1 => METEORA_DAMM_V1_PROGRAM_ID,
            DexKind::MeteoraDammV2 => METEORA_DAMM_V2_PROGRAM_ID,
            _ => bail!("invalid dex kind for Meteora DAMM executor"),
        };

        // Build account metas based on version
        // DAMM V1 uses shared vault architecture (Mercurial) - 15 accounts
        // DAMM V2 uses direct token vaults - 14 accounts
        let accounts = match snapshot.descriptor.kind {
            DexKind::MeteoraDammV1 => {
                // Verify we have all V1-specific accounts
                let a_vault = damm_accounts.a_vault.ok_or_else(|| {
                    anyhow::anyhow!("missing a_vault for DAMM V1 swap")
                })?;
                let b_vault = damm_accounts.b_vault.ok_or_else(|| {
                    anyhow::anyhow!("missing b_vault for DAMM V1 swap")
                })?;
                let a_vault_lp = damm_accounts.a_vault_lp.ok_or_else(|| {
                    anyhow::anyhow!("missing a_vault_lp for DAMM V1 swap")
                })?;
                let b_vault_lp = damm_accounts.b_vault_lp.ok_or_else(|| {
                    anyhow::anyhow!("missing b_vault_lp for DAMM V1 swap")
                })?;
                let a_vault_lp_mint = damm_accounts.a_vault_lp_mint.ok_or_else(|| {
                    anyhow::anyhow!("missing a_vault_lp_mint for DAMM V1 swap")
                })?;
                let b_vault_lp_mint = damm_accounts.b_vault_lp_mint.ok_or_else(|| {
                    anyhow::anyhow!("missing b_vault_lp_mint for DAMM V1 swap")
                })?;
                let protocol_token_a_fee = damm_accounts.protocol_token_a_fee.ok_or_else(|| {
                    anyhow::anyhow!("missing protocol_token_a_fee for DAMM V1 swap")
                })?;
                let protocol_token_b_fee = damm_accounts.protocol_token_b_fee.ok_or_else(|| {
                    anyhow::anyhow!("missing protocol_token_b_fee for DAMM V1 swap")
                })?;

                // Determine which protocol fee account to use based on input token
                // If input is base (token A) -> use protocol_token_a_fee
                // If input is quote (token B) -> use protocol_token_b_fee
                let protocol_token_fee = if input_token_account == request.accounts.user_base_account {
                    protocol_token_a_fee
                } else {
                    protocol_token_b_fee
                };

                // DAMM V1 swap accounts (from IDL):
                // 1.  pool                 - Pool account (PDA)
                // 2.  userSourceToken      - User's input token account
                // 3.  userDestinationToken - User's output token account
                // 4.  aVault               - Vault account for token A
                // 5.  bVault               - Vault account for token B
                // 6.  aTokenVault          - Token vault account inside aVault
                // 7.  bTokenVault          - Token vault account inside bVault
                // 8.  aVaultLpMint         - LP token mint of vault A
                // 9.  bVaultLpMint         - LP token mint of vault B
                // 10. aVaultLp             - Pool's LP token account in vault A
                // 11. bVaultLp             - Pool's LP token account in vault B
                // 12. protocolTokenFee     - Protocol fee token account (matches input token)
                // 13. user                 - User account (signer)
                // 14. vaultProgram         - Vault program ID
                // 15. tokenProgram         - Token program ID
                vec![
                    AccountMeta::new(snapshot.descriptor.address, false),     // 1. pool
                    AccountMeta::new(input_token_account, false),             // 2. userSourceToken
                    AccountMeta::new(output_token_account, false),            // 3. userDestinationToken
                    AccountMeta::new(a_vault, false),                         // 4. aVault
                    AccountMeta::new(b_vault, false),                         // 5. bVault
                    AccountMeta::new(damm_accounts.token_vault_a, false),     // 6. aTokenVault
                    AccountMeta::new(damm_accounts.token_vault_b, false),     // 7. bTokenVault
                    AccountMeta::new(a_vault_lp_mint, false),                 // 8. aVaultLpMint
                    AccountMeta::new(b_vault_lp_mint, false),                 // 9. bVaultLpMint
                    AccountMeta::new(a_vault_lp, false),                      // 10. aVaultLp
                    AccountMeta::new(b_vault_lp, false),                      // 11. bVaultLp
                    AccountMeta::new(protocol_token_fee, false),              // 12. protocolTokenFee
                    AccountMeta::new_readonly(request.accounts.payer, true),  // 13. user (signer)
                    AccountMeta::new_readonly(crate::constants::METEORA_VAULT_PROGRAM_ID, false), // 14. vaultProgram
                    AccountMeta::new_readonly(spl_token::ID, false),          // 15. tokenProgram
                ]
            }
            DexKind::MeteoraDammV2 => {
                let event_authority = Pubkey::find_program_address(&[b"__event_authority"], &program_id).0;
                
                // DAMM V2 swap accounts (current implementation)
                vec![
                    AccountMeta::new_readonly(METEORA_DAMM_POOL_AUTHORITY, false), // 1. pool_authority
                    AccountMeta::new(snapshot.descriptor.address, false),          // 2. pool
                    AccountMeta::new(input_token_account, false),                  // 3. input_token_account
                    AccountMeta::new(output_token_account, false),                 // 4. output_token_account
                    AccountMeta::new(damm_accounts.token_vault_a, false),          // 5. token_a_vault
                    AccountMeta::new(damm_accounts.token_vault_b, false),          // 6. token_b_vault
                    AccountMeta::new_readonly(snapshot.base_mint, false),          // 7. token_a_mint
                    AccountMeta::new_readonly(snapshot.quote_mint, false),         // 8. token_b_mint
                    AccountMeta::new_readonly(request.accounts.payer, true),       // 9. payer (signer)
                    AccountMeta::new_readonly(damm_accounts.token_a_program, false), // 10. token_a_program
                    AccountMeta::new_readonly(damm_accounts.token_b_program, false), // 11. token_b_program
                    AccountMeta::new_readonly(program_id, false),                  // 12. referral_token_account (placeholder)
                    AccountMeta::new_readonly(event_authority, false),             // 13. event_authority
                    AccountMeta::new_readonly(program_id, false),                  // 14. program
                ]
            }
            _ => unreachable!(),
        };

        #[repr(C, packed)]
        struct SwapParams {
            discriminator: [u8; 8],
            amount_in: u64,
            minimum_amount_out: u64,
        }

        let swap_params = SwapParams {
            discriminator: SWAP_DISCRIMINATOR,
            amount_in,
            minimum_amount_out,
        };

        let data = unsafe {
            std::slice::from_raw_parts(
                &swap_params as *const _ as *const u8,
                std::mem::size_of::<SwapParams>(),
            )
            .to_vec()
        };

        let instruction = Instruction {
            program_id,
            accounts,
            data,
        };

        let side_str = match request.side {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        };

        // Convert raw amounts back to float for description
        let amount_in_f = amount_in as f64 / 10f64.powi(norm_quote_decimals as i32);
        let min_out_f = minimum_amount_out as f64 / 10f64.powi(norm_base_decimals as i32);

        let version = match snapshot.descriptor.kind {
            DexKind::MeteoraDammV1 => "V1",
            DexKind::MeteoraDammV2 => "V2",
            _ => "?",
        };

        let description = format!(
            "Meteora DAMM {} {} in={:.6} min_out={:.6} slippage={:.2}bps",
            version,
            side_str,
            amount_in_f,
            min_out_f,
            request.max_slippage_bps,
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

