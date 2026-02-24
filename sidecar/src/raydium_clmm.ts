import type { Request, Response } from 'express';
import { Connection, PublicKey } from '@solana/web3.js';
import BN from 'bn.js';
import { PoolInfoLayout, getPdaExBitmapAccount, swapInstruction } from '@raydium-io/raydium-sdk';

let cachedConnection: Connection | null = null;

function getConnection(): Connection {
  if (cachedConnection) {
    return cachedConnection;
  }
  const url = process.env.RPC_URL || 'https://api.mainnet-beta.solana.com';
  cachedConnection = new Connection(url, 'confirmed');
  return cachedConnection;
}

async function getMintDecimals(connection: Connection, mint: PublicKey): Promise<number> {
  const info = await connection.getParsedAccountInfo(mint, 'confirmed');
  if (!info.value) {
    throw new Error('mint account not found');
  }
  const data: any = info.value.data as any;
  const parsed = (data as any).parsed;
  const decimals = parsed?.info?.decimals;
  if (typeof decimals !== 'number') {
    throw new Error('failed to read mint decimals');
  }
  return decimals;
}

const TICK_ARRAY_SIZE = 60;

function tickArrayStartIndex(tickIndex: number, tickSpacing: number): number {
  const span = tickSpacing * TICK_ARRAY_SIZE;
  if (span === 0) return 0;
  let start = Math.trunc(tickIndex / span) * span;
  if (tickIndex < 0 && tickIndex % span !== 0) {
    start -= span;
  }
  return start;
}

function tickArrayAddress(
  programId: PublicKey,
  pool: PublicKey,
  startTickIndex: number,
): PublicKey {
  const tickBytes = Buffer.alloc(4);
  tickBytes.writeInt32BE(startTickIndex, 0);
  const [address] = PublicKey.findProgramAddressSync(
    [Buffer.from('tick_array'), pool.toBuffer(), tickBytes],
    programId,
  );
  return address;
}

function computeTickArrayAddress(
  programId: PublicKey,
  pool: PublicKey,
  tickSpacing: number,
  tickCurrent: number,
  offset: number,
  zeroForOne: boolean,
): PublicKey {
  const shift = zeroForOne ? 0 : tickSpacing;
  const startIndex = tickArrayStartIndex(tickCurrent + shift, tickSpacing);
  const offsetIndex = startIndex + offset * tickSpacing * TICK_ARRAY_SIZE;
  return tickArrayAddress(programId, pool, offsetIndex);
}

const MIN_SQRT_PRICE_X64 = new BN('4295048016');
const MAX_SQRT_PRICE_X64 = new BN('79226673515401279992447579055');

function computeSqrtPriceLimit(currentSqrtPrice: BN, zeroForOne: boolean): BN {
  if (zeroForOne) {
    return MIN_SQRT_PRICE_X64.gt(new BN(1)) ? MIN_SQRT_PRICE_X64 : new BN(1);
  }
  const doubled = currentSqrtPrice.muln(2);
  return doubled.gt(MAX_SQRT_PRICE_X64) ? MAX_SQRT_PRICE_X64 : doubled;
}

type RaydiumSwapIxRequest = {
  pool: string;
  amount: string;
  other_amount_threshold: string;
  is_base_input: boolean;
  zero_for_one: boolean;
  user: string;
  input_token_account: string;
  output_token_account: string;
};

export async function buildRaydiumSwapIx(req: Request, res: Response): Promise<void> {
  try {
    const body = req.body as RaydiumSwapIxRequest;

    if (
      !body ||
      !body.pool ||
      !body.amount ||
      !body.other_amount_threshold ||
      !body.user ||
      !body.input_token_account ||
      !body.output_token_account
    ) {
      res.status(400).json({ ok: false, error: 'missing required fields' });
      return;
    }

    const poolPubkey = new PublicKey(body.pool);
    const userPubkey = new PublicKey(body.user);
    const inputTokenAccount = new PublicKey(body.input_token_account);
    const outputTokenAccount = new PublicKey(body.output_token_account);

    let amount: BN;
    let otherAmountThreshold: BN;
    try {
      amount = new BN(body.amount, 10);
      otherAmountThreshold = new BN(body.other_amount_threshold, 10);
    } catch {
      res.status(400).json({ ok: false, error: 'invalid amount format' });
      return;
    }

    if (amount.lte(new BN(0)) || otherAmountThreshold.lte(new BN(0))) {
      res
        .status(400)
        .json({ ok: false, error: 'amount and other_amount_threshold must be positive' });
      return;
    }

    const zeroForOne = !!body.zero_for_one;
    const isBaseInput = !!body.is_base_input;

    const connection = getConnection();
    const accountInfo = await connection.getAccountInfo(poolPubkey, 'confirmed');
    if (!accountInfo || !accountInfo.data) {
      res.status(404).json({ ok: false, error: 'pool account not found' });
      return;
    }

    const poolInfo: any = PoolInfoLayout.decode(accountInfo.data);
    const programId = accountInfo.owner as PublicKey;

    const mintA = poolInfo.mintA as PublicKey;
    const mintB = poolInfo.mintB as PublicKey;
    const vaultA = poolInfo.vaultA as PublicKey;
    const vaultB = poolInfo.vaultB as PublicKey;
    const ammConfig = poolInfo.ammConfig as PublicKey;
    const observationId = poolInfo.observationId as PublicKey;
    const tickSpacing = poolInfo.tickSpacing as number;
    const tickCurrent = poolInfo.tickCurrent as number;
    const sqrtPriceX64 = poolInfo.sqrtPriceX64 as BN;

    const inputMint = zeroForOne ? mintA : mintB;
    const outputMint = zeroForOne ? mintB : mintA;
    const inputVault = zeroForOne ? vaultA : vaultB;
    const outputVault = zeroForOne ? vaultB : vaultA;

    const tickArray0 = computeTickArrayAddress(
      programId,
      poolPubkey,
      tickSpacing,
      tickCurrent,
      0,
      zeroForOne,
    );
    const tickArray1 = computeTickArrayAddress(
      programId,
      poolPubkey,
      tickSpacing,
      tickCurrent,
      1,
      zeroForOne,
    );
    const tickArray2 = computeTickArrayAddress(
      programId,
      poolPubkey,
      tickSpacing,
      tickCurrent,
      2,
      zeroForOne,
    );

    const sqrtPriceLimitX64 = computeSqrtPriceLimit(sqrtPriceX64, zeroForOne);
    const exBitmapPda = getPdaExBitmapAccount(programId, poolPubkey).publicKey;

    const ix = swapInstruction(
      programId,
      userPubkey,
      poolPubkey,
      ammConfig,
      inputTokenAccount,
      outputTokenAccount,
      inputVault,
      outputVault,
      inputMint,
      outputMint,
      [tickArray0, tickArray1, tickArray2],
      observationId,
      amount,
      otherAmountThreshold,
      sqrtPriceLimitX64,
      isBaseInput,
      exBitmapPda,
    );

    const response = {
      ok: true,
      dex: 'raydium_clmm',
      program_id: ix.programId.toBase58(),
      accounts: ix.keys.map((k) => ({
        pubkey: k.pubkey.toBase58(),
        is_signer: k.isSigner,
        is_writable: k.isWritable,
      })),
      data_base64: Buffer.from(ix.data).toString('base64'),
    };

    res.json(response);
  } catch (err: any) {
    const message = err && err.message ? String(err.message) : String(err);
    console.error('Error in /raydium/build_swap_ix:', message);
    res.status(500).json({ ok: false, error: message });
  }
}

// Introspect a Raydium CLMM pool and return its details for snapshot validation
export async function introspectRaydiumPool(req: Request, res: Response): Promise<void> {
  try {
    const { pool } = req.body as { pool?: string };
    if (!pool) {
      res.status(400).json({ ok: false, error: 'missing pool address' });
      return;
    }

    const poolPubkey = new PublicKey(pool);
    const connection = getConnection();
    const accountInfo = await connection.getAccountInfo(poolPubkey, 'confirmed');
    if (!accountInfo || !accountInfo.data) {
      res.status(404).json({ ok: false, error: 'pool account not found' });
      return;
    }

    const poolInfo: any = PoolInfoLayout.decode(accountInfo.data);
    const programId = accountInfo.owner as PublicKey;

    const mint0 = poolInfo.mintA as PublicKey;
    const mint1 = poolInfo.mintB as PublicKey;
    const vault0 = poolInfo.vaultA as PublicKey;
    const vault1 = poolInfo.vaultB as PublicKey;
    const tickSpacing = poolInfo.tickSpacing as number;
    const tickCurrent = poolInfo.tickCurrent as number;
    const sqrtPriceX64 = poolInfo.sqrtPriceX64 as BN;
    const liquidity = poolInfo.liquidity as BN | undefined;
    const tradeFeeRate = Number(poolInfo.tradeFeeRate ?? 0);
    const protocolFeeRate = Number(poolInfo.protocolFeeRate ?? 0);
    const fundFeeRate = Number(poolInfo.fundFeeRate ?? 0);

    const [decimals0, decimals1, slot] = await Promise.all([
      getMintDecimals(connection, mint0),
      getMintDecimals(connection, mint1),
      connection.getSlot(),
    ]);

    // Vault balances are optional for our use-case (we mainly care about price).
    // If the vault accounts are missing or not SPL Token accounts, default to "0"
    // instead of failing the entire introspection.
    let reserve0Amount = '0';
    try {
      const vault0Balance = await connection.getTokenAccountBalance(vault0);
      reserve0Amount = vault0Balance.value.amount;
    } catch (e) {
      console.warn('Raydium introspect: failed to fetch vault_0 balance, defaulting to 0', e);
    }

    let reserve1Amount = '0';
    try {
      const vault1Balance = await connection.getTokenAccountBalance(vault1);
      reserve1Amount = vault1Balance.value.amount;
    } catch (e) {
      console.warn('Raydium introspect: failed to fetch vault_1 balance, defaulting to 0', e);
    }

    const sqrtPriceNum = Number(sqrtPriceX64.toString()) / Math.pow(2, 64);
    const priceRaw = sqrtPriceNum * sqrtPriceNum;
    const price = priceRaw * Math.pow(10, decimals0 - decimals1);

    const response = {
      ok: true,
      dex: 'raydium_clmm',
      pool,
      program_id: programId.toBase58(),
      token_mint_0: mint0.toBase58(),
      token_mint_1: mint1.toBase58(),
      vault_0: vault0.toBase58(),
      vault_1: vault1.toBase58(),
      decimals_0: decimals0,
      decimals_1: decimals1,
      reserve_0: reserve0Amount,
      reserve_1: reserve1Amount,
      sqrt_price_x64: sqrtPriceX64.toString(),
      price: price.toString(),
      liquidity: liquidity ? liquidity.toString() : '0',
      tick_spacing: tickSpacing,
      tick_current_index: tickCurrent,
      fee_rate: tradeFeeRate,
      protocol_fee_rate: protocolFeeRate,
      fund_fee_rate: fundFeeRate,
      slot,
    };

    res.json(response);
  } catch (err: any) {
    const message = err && err.message ? String(err.message) : String(err);
    console.error('Error in /raydium/introspect:', message);
    res.status(500).json({ ok: false, error: message });
  }
}

// Batch introspect multiple Raydium CLMM pools in one request
export async function batchIntrospectRaydiumPools(req: Request, res: Response): Promise<void> {
  try {
    const { pools } = req.body as { pools?: string[] };
    if (!pools || !Array.isArray(pools) || pools.length === 0) {
      res.status(400).json({ ok: false, error: 'missing or empty pools array' });
      return;
    }

    const connection = getConnection();
    const results: Array<{
      pool: string;
      ok: boolean;
      tick_current_index?: number;
      tick_spacing?: number;
      error?: string;
    }> = [];

    // Process pools sequentially to avoid rate limiting
    for (const pool of pools) {
      try {
        const poolPubkey = new PublicKey(pool);
        const accountInfo = await connection.getAccountInfo(poolPubkey, 'confirmed');

        if (!accountInfo || !accountInfo.data) {
          results.push({ pool, ok: false, error: 'pool account not found' });
          continue;
        }

        const poolInfo: any = PoolInfoLayout.decode(accountInfo.data);
        results.push({
          pool,
          ok: true,
          tick_current_index: poolInfo.tickCurrent as number,
          tick_spacing: poolInfo.tickSpacing as number,
        });

        // Small delay between pools (50ms)
        await new Promise(resolve => setTimeout(resolve, 50));
      } catch (err: any) {
        const message = err && err.message ? String(err.message) : String(err);
        console.warn(`Batch introspect failed for Raydium pool ${pool}: ${message}`);
        results.push({ pool, ok: false, error: message });
      }
    }

    res.json({
      ok: true,
      count: results.length,
      success_count: results.filter(r => r.ok).length,
      results,
    });
  } catch (err: any) {
    const message = err && err.message ? String(err.message) : String(err);
    console.error('Error in /raydium/batch_introspect:', message);
    res.status(500).json({ ok: false, error: message });
  }
}
