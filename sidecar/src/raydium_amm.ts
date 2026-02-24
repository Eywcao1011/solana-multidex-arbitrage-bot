import type { Request, Response } from 'express';
import { Connection, PublicKey } from '@solana/web3.js';
import BN from 'bn.js';
import {
  LIQUIDITY_STATE_LAYOUT_V4,
  Liquidity,
  MARKET_STATE_LAYOUT_V3,
  jsonInfo2PoolKeys,
} from '@raydium-io/raydium-sdk';

const RAYDIUM_AMM_PROGRAM_ID = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
const SERUM_PROGRAM_ID = new PublicKey('srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX');

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

// Derive the AMM authority PDA
function getAssociatedAuthority(programId: PublicKey): { publicKey: PublicKey; nonce: number } {
  let nonce = 0;
  let authority: PublicKey | null = null;

  // Find the authority PDA by trying different nonces
  for (let i = 0; i < 256; i++) {
    try {
      const seeds = [
        // Empty seeds array with nonce
        Buffer.alloc(0),
      ];
      // The Raydium AMM uses a specific seed format
      const [pda] = PublicKey.findProgramAddressSync(
        [
          Buffer.from([
            97, 109, 109, 32, 97, 117, 116, 104, 111, 114, 105, 116, 121, // "amm authority"
          ]),
        ],
        programId,
      );
      return { publicKey: pda, nonce: 0 };
    } catch {
      continue;
    }
  }

  // Fallback: use the SDK's method
  return Liquidity.getAssociatedAuthority({ programId });
}

// Introspect a Raydium AMM V4 pool and return its details
export async function introspectRaydiumAmmPool(req: Request, res: Response): Promise<void> {
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

    const programId = accountInfo.owner as PublicKey;

    // Verify this is an AMM V4 pool
    if (!programId.equals(RAYDIUM_AMM_PROGRAM_ID)) {
      res.status(400).json({
        ok: false,
        error: `not a Raydium AMM V4 pool. Owner: ${programId.toBase58()}`,
      });
      return;
    }

    const poolState = LIQUIDITY_STATE_LAYOUT_V4.decode(accountInfo.data);

    const baseMint = poolState.baseMint;
    const quoteMint = poolState.quoteMint;
    const baseVault = poolState.baseVault;
    const quoteVault = poolState.quoteVault;
    const lpMint = poolState.lpMint;
    const openOrders = poolState.openOrders;
    const targetOrders = poolState.targetOrders;
    const marketId = poolState.marketId;
    const marketProgramId = poolState.marketProgramId;

    const baseDecimal = Number(poolState.baseDecimal.toString());
    const quoteDecimal = Number(poolState.quoteDecimal.toString());
    const status = Number(poolState.status.toString());

    // Fee info
    const tradeFeeNumerator = Number(poolState.tradeFeeNumerator.toString());
    const tradeFeeDenominator = Number(poolState.tradeFeeDenominator.toString());
    const swapFeeNumerator = Number(poolState.swapFeeNumerator.toString());
    const swapFeeDenominator = Number(poolState.swapFeeDenominator.toString());

    // Get vault balances
    let baseReserve = '0';
    let quoteReserve = '0';
    try {
      const [baseBalance, quoteBalance] = await Promise.all([
        connection.getTokenAccountBalance(baseVault),
        connection.getTokenAccountBalance(quoteVault),
      ]);
      baseReserve = baseBalance.value.amount;
      quoteReserve = quoteBalance.value.amount;
    } catch (e) {
      console.warn('Raydium AMM introspect: failed to fetch vault balances, defaulting to 0', e);
    }

    // Read Serum market account to get bids/asks/event_queue/vaults
    let serumBids = '';
    let serumAsks = '';
    let serumEventQueue = '';
    let serumCoinVault = '';
    let serumPcVault = '';
    let serumVaultSigner = '';
    try {
      const marketAccountInfo = await connection.getAccountInfo(marketId, 'confirmed');
      if (marketAccountInfo && marketAccountInfo.data) {
        const marketState = MARKET_STATE_LAYOUT_V3.decode(marketAccountInfo.data);
        serumBids = marketState.bids.toBase58();
        serumAsks = marketState.asks.toBase58();
        serumEventQueue = marketState.eventQueue.toBase58();
        serumCoinVault = marketState.baseVault.toBase58();
        serumPcVault = marketState.quoteVault.toBase58();
        // Derive vault signer
        serumVaultSigner = PublicKey.createProgramAddressSync(
          [
            marketId.toBuffer(),
            marketState.vaultSignerNonce.toArrayLike(Buffer, 'le', 8),
          ],
          marketProgramId,
        ).toBase58();
      }
    } catch (e) {
      console.warn('Raydium AMM introspect: failed to read Serum market account', e);
    }

    const slot = await connection.getSlot();

    // Calculate price: quote_reserve / base_reserve (adjusted for decimals)
    const baseReserveNum = Number(baseReserve) / Math.pow(10, baseDecimal);
    const quoteReserveNum = Number(quoteReserve) / Math.pow(10, quoteDecimal);
    const price = baseReserveNum > 0 ? quoteReserveNum / baseReserveNum : 0;

    // Fee rate calculation
    const feeRate = tradeFeeDenominator > 0 ? tradeFeeNumerator / tradeFeeDenominator : 0;

    const response = {
      ok: true,
      dex: 'raydium_amm_v4',
      pool,
      program_id: programId.toBase58(),
      base_mint: baseMint.toBase58(),
      quote_mint: quoteMint.toBase58(),
      base_vault: baseVault.toBase58(),
      quote_vault: quoteVault.toBase58(),
      lp_mint: lpMint.toBase58(),
      open_orders: openOrders.toBase58(),
      target_orders: targetOrders.toBase58(),
      market_id: marketId.toBase58(),
      market_program_id: marketProgramId.toBase58(),
      // Serum market accounts (for local build IX)
      serum_bids: serumBids || undefined,
      serum_asks: serumAsks || undefined,
      serum_event_queue: serumEventQueue || undefined,
      serum_coin_vault: serumCoinVault || undefined,
      serum_pc_vault: serumPcVault || undefined,
      serum_vault_signer: serumVaultSigner || undefined,
      base_decimals: baseDecimal,
      quote_decimals: quoteDecimal,
      base_reserve: baseReserve,
      quote_reserve: quoteReserve,
      price: price.toString(),
      fee_rate: feeRate,
      trade_fee_numerator: tradeFeeNumerator,
      trade_fee_denominator: tradeFeeDenominator,
      swap_fee_numerator: swapFeeNumerator,
      swap_fee_denominator: swapFeeDenominator,
      status,
      slot,
    };

    res.json(response);
  } catch (err: any) {
    const message = err && err.message ? String(err.message) : String(err);
    console.error('Error in /raydium_amm/introspect:', message);
    res.status(500).json({ ok: false, error: message });
  }
}

type RaydiumAmmSwapIxRequest = {
  pool: string;
  amount_in: string;
  min_amount_out: string;
  side: 'buy' | 'sell'; // buy = quote->base, sell = base->quote
  user: string;
  user_token_in: string;
  user_token_out: string;
};

// Build a swap instruction for Raydium AMM V4
export async function buildRaydiumAmmSwapIx(req: Request, res: Response): Promise<void> {
  try {
    const body = req.body as RaydiumAmmSwapIxRequest;

    if (
      !body ||
      !body.pool ||
      !body.amount_in ||
      !body.min_amount_out ||
      !body.side ||
      !body.user ||
      !body.user_token_in ||
      !body.user_token_out
    ) {
      res.status(400).json({ ok: false, error: 'missing required fields' });
      return;
    }

    const poolPubkey = new PublicKey(body.pool);
    const userPubkey = new PublicKey(body.user);
    const userTokenIn = new PublicKey(body.user_token_in);
    const userTokenOut = new PublicKey(body.user_token_out);

    let amountIn: BN;
    let minAmountOut: BN;
    try {
      amountIn = new BN(body.amount_in, 10);
      minAmountOut = new BN(body.min_amount_out, 10);
    } catch {
      res.status(400).json({ ok: false, error: 'invalid amount format' });
      return;
    }

    if (amountIn.lte(new BN(0)) || minAmountOut.lt(new BN(0))) {
      res.status(400).json({ ok: false, error: 'amount_in must be positive' });
      return;
    }

    const connection = getConnection();
    const accountInfo = await connection.getAccountInfo(poolPubkey, 'confirmed');
    if (!accountInfo || !accountInfo.data) {
      res.status(404).json({ ok: false, error: 'pool account not found' });
      return;
    }

    const programId = accountInfo.owner as PublicKey;
    const poolState = LIQUIDITY_STATE_LAYOUT_V4.decode(accountInfo.data);

    // Get market account data for additional keys
    const marketId = poolState.marketId;
    const marketProgramId = poolState.marketProgramId;

    const marketAccountInfo = await connection.getAccountInfo(marketId, 'confirmed');
    if (!marketAccountInfo || !marketAccountInfo.data) {
      res.status(404).json({ ok: false, error: 'market account not found' });
      return;
    }

    const marketState = MARKET_STATE_LAYOUT_V3.decode(marketAccountInfo.data);

    // Get the AMM authority
    const authority = Liquidity.getAssociatedAuthority({ programId }).publicKey;

    // Build pool keys object
    const poolKeys = {
      id: poolPubkey,
      baseMint: poolState.baseMint,
      quoteMint: poolState.quoteMint,
      lpMint: poolState.lpMint,
      baseDecimals: Number(poolState.baseDecimal.toString()),
      quoteDecimals: Number(poolState.quoteDecimal.toString()),
      lpDecimals: Number(poolState.baseDecimal.toString()), // LP decimals usually same as base
      version: 4 as const,
      programId,
      authority,
      openOrders: poolState.openOrders,
      targetOrders: poolState.targetOrders,
      baseVault: poolState.baseVault,
      quoteVault: poolState.quoteVault,
      withdrawQueue: poolState.withdrawQueue,
      lpVault: poolState.lpVault,
      marketVersion: 3 as const,
      marketProgramId,
      marketId,
      marketAuthority: PublicKey.createProgramAddressSync(
        [
          marketId.toBuffer(),
          marketState.vaultSignerNonce.toArrayLike(Buffer, 'le', 8),
        ],
        marketProgramId,
      ),
      marketBaseVault: marketState.baseVault,
      marketQuoteVault: marketState.quoteVault,
      marketBids: marketState.bids,
      marketAsks: marketState.asks,
      marketEventQueue: marketState.eventQueue,
      lookupTableAccount: PublicKey.default,
    };

    // Build swap instruction using SDK
    const { innerTransaction } = Liquidity.makeSwapFixedInInstruction(
      {
        poolKeys,
        userKeys: {
          tokenAccountIn: userTokenIn,
          tokenAccountOut: userTokenOut,
          owner: userPubkey,
        },
        amountIn,
        minAmountOut,
      },
      poolKeys.version,
    );

    const ix = innerTransaction.instructions[0];

    const response = {
      ok: true,
      dex: 'raydium_amm_v4',
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
    console.error('Error in /raydium_amm/build_swap_ix:', message);
    res.status(500).json({ ok: false, error: message });
  }
}

// Batch introspect multiple Raydium AMM V4 pools in one request
export async function batchIntrospectRaydiumAmmPools(req: Request, res: Response): Promise<void> {
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
      base_reserve?: string;
      quote_reserve?: string;
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

        const poolState = LIQUIDITY_STATE_LAYOUT_V4.decode(accountInfo.data);

        // Get vault balances
        let baseReserve = '0';
        let quoteReserve = '0';
        try {
          const [baseBalance, quoteBalance] = await Promise.all([
            connection.getTokenAccountBalance(poolState.baseVault),
            connection.getTokenAccountBalance(poolState.quoteVault),
          ]);
          baseReserve = baseBalance.value.amount;
          quoteReserve = quoteBalance.value.amount;
        } catch (e) {
          // Non-fatal: continue with 0 reserves
        }

        results.push({
          pool,
          ok: true,
          base_reserve: baseReserve,
          quote_reserve: quoteReserve,
        });

        // Small delay between pools (50ms)
        await new Promise(resolve => setTimeout(resolve, 50));
      } catch (err: any) {
        const message = err && err.message ? String(err.message) : String(err);
        console.warn(`Batch introspect failed for Raydium AMM pool ${pool}: ${message}`);
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
    console.error('Error in /raydium_amm/batch_introspect:', message);
    res.status(500).json({ ok: false, error: message });
  }
}
