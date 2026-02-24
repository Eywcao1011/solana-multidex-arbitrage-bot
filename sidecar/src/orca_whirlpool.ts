import type { Request, Response } from 'express';
import { Connection, PublicKey, Keypair } from '@solana/web3.js';
import BN from 'bn.js';
import { getAssociatedTokenAddressSync } from '@solana/spl-token';
import {
  WhirlpoolContext,
  buildWhirlpoolClient,
  swapQuoteByInputToken,
  ORCA_WHIRLPOOL_PROGRAM_ID,
  PriceMath,
  SwapUtils,
  WhirlpoolIx,
} from '@orca-so/whirlpools-sdk';
import { Percentage } from '@orca-so/common-sdk';

let cachedConnection: Connection | null = null;
let cachedContext: WhirlpoolContext | null = null;
let cachedClient: ReturnType<typeof buildWhirlpoolClient> | null = null;

function getConnection(): Connection {
  if (cachedConnection) return cachedConnection;
  const url = process.env.RPC_URL || 'https://api.mainnet-beta.solana.com';
  cachedConnection = new Connection(url, 'confirmed');
  return cachedConnection;
}

function getContext(): WhirlpoolContext {
  if (cachedContext) return cachedContext;
  const connection = getConnection();
  const dummyKeypair = Keypair.generate();
  const dummyWallet = {
    publicKey: dummyKeypair.publicKey,
    signTransaction: async (tx: any) => tx,
    signAllTransactions: async (txs: any[]) => txs,
  };
  cachedContext = WhirlpoolContext.from(connection, dummyWallet as any);
  return cachedContext;
}

function getClient() {
  if (cachedClient) return cachedClient;
  const ctx = getContext();
  cachedClient = buildWhirlpoolClient(ctx);
  return cachedClient;
}

// Introspect a Whirlpool pool and return its details
export async function introspectOrcaPool(req: Request, res: Response): Promise<void> {
  try {
    const { pool } = req.body as { pool?: string };
    if (!pool) {
      res.status(400).json({ ok: false, error: 'missing pool address' });
      return;
    }

    const poolPubkey = new PublicKey(pool);
    const client = getClient();
    const connection = getConnection();

    const whirlpool = await client.getPool(poolPubkey);
    const poolData = whirlpool.getData();

    const tokenAInfo = whirlpool.getTokenAInfo();
    const tokenBInfo = whirlpool.getTokenBInfo();
    const vaultAInfo = whirlpool.getTokenVaultAInfo();
    const vaultBInfo = whirlpool.getTokenVaultBInfo();

    const mintAStr = tokenAInfo.mint.toBase58();
    const mintBStr = tokenBInfo.mint.toBase58();
    const vaultAStr = poolData.tokenVaultA.toBase58();
    const vaultBStr = poolData.tokenVaultB.toBase58();
    const decimalsA = tokenAInfo.decimals;
    const decimalsB = tokenBInfo.decimals;

    const price = PriceMath.sqrtPriceX64ToPrice(poolData.sqrtPrice, decimalsA, decimalsB);
    const priceStr = price.toString();

    const reserveAStr = vaultAInfo.amount.toString();
    const reserveBStr = vaultBInfo.amount.toString();
    const slot = await connection.getSlot();

    const response = {
      ok: true,
      dex: 'orca_whirlpool',
      pool: pool,
      program_id: 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
      token_mint_a: mintAStr,
      token_mint_b: mintBStr,
      token_vault_a: vaultAStr,
      token_vault_b: vaultBStr,
      decimals_a: decimalsA,
      decimals_b: decimalsB,
      reserve_a: reserveAStr,
      reserve_b: reserveBStr,
      sqrt_price_x64: poolData.sqrtPrice.toString(),
      price: priceStr,
      liquidity: poolData.liquidity.toString(),
      tick_spacing: poolData.tickSpacing,
      tick_current_index: poolData.tickCurrentIndex,
      fee_rate: poolData.feeRate,
      protocol_fee_rate: poolData.protocolFeeRate,
      slot: slot,
    };

    res.json(response);
  } catch (err: any) {
    const message = err && err.message ? String(err.message) : String(err);
    console.error('Error in /orca/introspect:', message);
    res.status(500).json({ ok: false, error: message });
  }
}

type OrcaSwapIxRequest = {
  pool: string;
  side: 'in_base' | 'in_quote';
  amount_in: string;
  min_amount_out: string;
  user: string;
  user_token_in?: string;
  user_token_out?: string;
};

export async function buildOrcaSwapIx(req: Request, res: Response): Promise<void> {
  try {
    const body = req.body as OrcaSwapIxRequest;

    if (!body || !body.pool || !body.side || !body.amount_in || !body.min_amount_out || !body.user) {
      res.status(400).json({ ok: false, error: 'missing required fields' });
      return;
    }

    if (body.side !== 'in_base' && body.side !== 'in_quote') {
      res.status(400).json({ ok: false, error: 'invalid side, must be in_base or in_quote' });
      return;
    }
    const poolPubkey = new PublicKey(body.pool);

    let amountInBig: bigint;
    let minOutBig: bigint;
    try {
      amountInBig = BigInt(body.amount_in);
      minOutBig = BigInt(body.min_amount_out);
    } catch {
      res.status(400).json({ ok: false, error: 'invalid amount format' });
      return;
    }

    if (amountInBig <= 0n || minOutBig <= 0n) {
      res.status(400).json({ ok: false, error: 'amount_in and min_amount_out must be positive' });
      return;
    }

    const client = getClient();
    const ctx = getContext();
    const whirlpool = await client.getPool(poolPubkey);
    const poolData = whirlpool.getData();

    const tokenAInfo = whirlpool.getTokenAInfo();
    const tokenBInfo = whirlpool.getTokenBInfo();

    const inputMint = body.side === 'in_base' ? tokenAInfo.mint : tokenBInfo.mint;
    const outputMint = body.side === 'in_base' ? tokenBInfo.mint : tokenAInfo.mint;
    const inputTokenProgram = body.side === 'in_base' ? tokenAInfo.tokenProgram : tokenBInfo.tokenProgram;
    const outputTokenProgram = body.side === 'in_base' ? tokenBInfo.tokenProgram : tokenAInfo.tokenProgram;

    const tokenAmount = new BN(body.amount_in);
    const slippage = Percentage.fromFraction(1, 100); // 1%
    const swapQuote = await swapQuoteByInputToken(
      whirlpool,
      inputMint,
      tokenAmount,
      slippage,
      ORCA_WHIRLPOOL_PROGRAM_ID,
      ctx.fetcher,
    );

    const userPubkey = new PublicKey(body.user);
    const inputAccount = body.user_token_in
      ? new PublicKey(body.user_token_in)
      : getAssociatedTokenAddressSync(inputMint, userPubkey, false, inputTokenProgram);
    const outputAccount = body.user_token_out
      ? new PublicKey(body.user_token_out)
      : getAssociatedTokenAddressSync(outputMint, userPubkey, false, outputTokenProgram);

    const params = SwapUtils.getSwapParamsFromQuote(
      swapQuote,
      ctx,
      whirlpool,
      inputAccount,
      outputAccount,
      userPubkey,
    );
    const ix: any = WhirlpoolIx.swapIx(ctx.program, params);

    const response = {
      ok: true,
      dex: 'orca_whirlpool',
      program_id: ix.programId.toBase58(),
      accounts: ix.keys.map((k: any) => ({
        pubkey: k.pubkey.toBase58(),
        is_signer: k.isSigner,
        is_writable: k.isWritable,
      })),
      data_base64: Buffer.from(ix.data).toString('base64'),
    };

    res.json(response);
  } catch (err: any) {
    const message = err && err.message ? String(err.message) : String(err);
    console.error('Error in /orca/build_swap_ix:', message);
    res.status(500).json({ ok: false, error: message });
  }
}

// Batch introspect multiple Orca Whirlpool pools in one request
export async function batchIntrospectOrcaPools(req: Request, res: Response): Promise<void> {
  try {
    const { pools } = req.body as { pools?: string[] };
    if (!pools || !Array.isArray(pools) || pools.length === 0) {
      res.status(400).json({ ok: false, error: 'missing or empty pools array' });
      return;
    }

    const client = getClient();
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
        const poolResult = await client.getPool(poolPubkey);

        if (!poolResult) {
          results.push({ pool, ok: false, error: 'pool not found' });
          continue;
        }

        const poolData = poolResult as any;
        results.push({
          pool,
          ok: true,
          tick_current_index: poolData.tickCurrentIndex || 0,
          tick_spacing: poolData.tickSpacing || 0,
        });

        // Small delay between pools (50ms)
        await new Promise(resolve => setTimeout(resolve, 50));
      } catch (err: any) {
        const message = err && err.message ? String(err.message) : String(err);
        console.warn(`Batch introspect failed for Orca pool ${pool}: ${message}`);
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
    console.error('Error in /orca/batch_introspect:', message);
    res.status(500).json({ ok: false, error: message });
  }
}
