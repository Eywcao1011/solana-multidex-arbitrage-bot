import type { Request, Response } from 'express';
import { Connection, PublicKey } from '@solana/web3.js';
import DLMM from '@meteora-ag/dlmm';
import BN from 'bn.js';

let cachedConnection: Connection | null = null;

function getConnection(): Connection {
  if (cachedConnection) {
    return cachedConnection;
  }
  const url = process.env.RPC_URL || 'https://api.mainnet-beta.solana.com';
  cachedConnection = new Connection(url, 'confirmed');
  return cachedConnection;
}

type MeteoraSwapIxRequest = {
  pool: string;
  side: 'in_base' | 'in_quote';
  amount_in: string;
  min_amount_out: string;
  user: string;
  user_token_in?: string;
  user_token_out?: string;
};

export async function buildMeteoraDlmmSwapIx(req: Request, res: Response): Promise<void> {
  try {
    const body = req.body as MeteoraSwapIxRequest;

    if (!body || !body.pool || !body.side || !body.amount_in || !body.min_amount_out || !body.user) {
      res.status(400).json({ ok: false, error: 'missing required fields' });
      return;
    }

    if (body.side !== 'in_base' && body.side !== 'in_quote') {
      res.status(400).json({ ok: false, error: 'invalid side, must be in_base or in_quote' });
      return;
    }

    const poolPubkey = new PublicKey(body.pool);
    const userPubkey = new PublicKey(body.user);

    const amountIn = new BN(body.amount_in, 10);
    const minOut = new BN(body.min_amount_out, 10);

    if (amountIn.lte(new BN(0)) || minOut.lte(new BN(0))) {
      res.status(400).json({ ok: false, error: 'amount_in and min_amount_out must be positive' });
      return;
    }

    const connection = getConnection();
    const dlmmPool = await DLMM.create(connection, poolPubkey);

    const swapForY = body.side === 'in_base';
    const binArrays = await dlmmPool.getBinArrayForSwap(swapForY, 20);
    const binArraysPubkey = binArrays.map((b: any) => b.publicKey as PublicKey);

    const tokenXMint: PublicKey = dlmmPool.lbPair.tokenXMint;
    const tokenYMint: PublicKey = dlmmPool.lbPair.tokenYMint;

    const inToken = body.side === 'in_base' ? tokenXMint : tokenYMint;
    const outToken = body.side === 'in_base' ? tokenYMint : tokenXMint;

    const tx = await dlmmPool.swap({
      inToken,
      outToken,
      inAmount: amountIn,
      minOutAmount: minOut,
      lbPair: dlmmPool.pubkey,
      user: userPubkey,
      binArraysPubkey,
    });

    if (!tx.instructions || tx.instructions.length === 0) {
      res.status(500).json({ ok: false, error: 'no instructions in built swap transaction' });
      return;
    }

    const ix = tx.instructions[0];

    const response = {
      ok: true,
      dex: 'meteora_dlmm',
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
    console.error('Error in /meteora/build_swap_ix:', message);
    res.status(500).json({ ok: false, error: message });
  }
}

// Introspect a Meteora DLMM pool and return its details for snapshot validation
export async function introspectMeteoraDlmmPool(req: Request, res: Response): Promise<void> {
  try {
    const { pool } = req.body as { pool?: string };
    if (!pool) {
      res.status(400).json({ ok: false, error: 'missing pool address' });
      return;
    }

    const poolPubkey = new PublicKey(pool);
    const connection = getConnection();
    const dlmmPool = await DLMM.create(connection, poolPubkey);

    const tokenXMint: PublicKey = dlmmPool.lbPair.tokenXMint;
    const tokenYMint: PublicKey = dlmmPool.lbPair.tokenYMint;
    const reserveX: PublicKey = dlmmPool.lbPair.reserveX;
    const reserveY: PublicKey = dlmmPool.lbPair.reserveY;
    const binStep: number = dlmmPool.lbPair.binStep;

    // Use SDK helper to get active bin and price per lamport
    const activeBin = await dlmmPool.getActiveBin();
    const activeId: number = activeBin.binId;

    // Get mint decimals
    const [mintXInfo, mintYInfo] = await Promise.all([
      connection.getParsedAccountInfo(tokenXMint, 'confirmed'),
      connection.getParsedAccountInfo(tokenYMint, 'confirmed'),
    ]);

    const decimalsX = (mintXInfo.value?.data as any)?.parsed?.info?.decimals ?? 9;
    const decimalsY = (mintYInfo.value?.data as any)?.parsed?.info?.decimals ?? 9;

    // Get vault balances
    let reserveXAmount = '0';
    let reserveYAmount = '0';
    try {
      const [balanceX, balanceY] = await Promise.all([
        connection.getTokenAccountBalance(reserveX),
        connection.getTokenAccountBalance(reserveY),
      ]);
      reserveXAmount = balanceX.value.amount;
      reserveYAmount = balanceY.value.amount;
    } catch (e) {
      console.warn('Meteora DLMM introspect: failed to fetch vault balances, defaulting to 0', e);
    }

    const slot = await connection.getSlot();

    // Calculate price using SDK: active bin price per lamport -> real price (Y per X)
    let price = 0;
    try {
      const priceStr = dlmmPool.fromPricePerLamport(Number(activeBin.price));
      const parsed = parseFloat(priceStr);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error('invalid price from fromPricePerLamport');
      }
      price = parsed;
    } catch (e) {
      console.warn('Meteora DLMM introspect: failed to compute price, defaulting to 0', e);
      price = 0;
    }

    // Get bin arrays for BOTH swap directions (50 each for aggressive caching)
    // X→Y direction (swapForY=true): used for Sell trades
    const binArraysXtoY = await dlmmPool.getBinArrayForSwap(true, 50);
    // Y→X direction (swapForY=false): used for Buy trades
    const binArraysYtoX = await dlmmPool.getBinArrayForSwap(false, 50);

    const xToYAddresses = binArraysXtoY.map((b: any) => (b.publicKey as PublicKey).toBase58());
    const yToXAddresses = binArraysYtoX.map((b: any) => (b.publicKey as PublicKey).toBase58());
    // Merged list for backward compatibility (deprecated)
    const binArrayAddresses = Array.from(new Set([...xToYAddresses, ...yToXAddresses]));

    const response = {
      ok: true,
      dex: 'meteora_dlmm',
      pool,
      program_id: dlmmPool.program.programId.toBase58(),
      token_mint_x: tokenXMint.toBase58(),
      token_mint_y: tokenYMint.toBase58(),
      reserve_x: reserveX.toBase58(),
      reserve_y: reserveY.toBase58(),
      decimals_x: decimalsX,
      decimals_y: decimalsY,
      reserve_x_amount: reserveXAmount,
      reserve_y_amount: reserveYAmount,
      bin_step: binStep,
      active_id: activeId,
      price: price.toString(),
      bin_arrays: binArrayAddresses,  // deprecated, use directional lists
      bin_arrays_x_to_y: xToYAddresses,  // for Sell (X→Y)
      bin_arrays_y_to_x: yToXAddresses,  // for Buy (Y→X)
      slot,
    };

    res.json(response);
  } catch (err: any) {
    const message = err && err.message ? String(err.message) : String(err);
    console.error('Error in /meteora/introspect:', message);
    res.status(500).json({ ok: false, error: message });
  }
}

// Batch introspect multiple Meteora DLMM pools in one request
// This reduces RPC pressure by processing pools sequentially with rate limiting
export async function batchIntrospectMeteoraDlmmPools(req: Request, res: Response): Promise<void> {
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
      active_id?: number;
      bin_arrays?: string[];  // deprecated
      bin_arrays_x_to_y?: string[];  // for Sell
      bin_arrays_y_to_x?: string[];  // for Buy
      error?: string;
    }> = [];

    // Process pools sequentially to avoid 429 rate limiting
    for (const pool of pools) {
      try {
        const poolPubkey = new PublicKey(pool);
        const dlmmPool = await DLMM.create(connection, poolPubkey);

        // Get active bin
        const activeBin = await dlmmPool.getActiveBin();
        const activeId: number = activeBin.binId;

        // Get bin arrays for BOTH swap directions (50 each for aggressive caching)
        // X→Y direction (swapForY=true): used for Sell trades  
        const binArraysXtoY = await dlmmPool.getBinArrayForSwap(true, 50);
        // Y→X direction (swapForY=false): used for Buy trades
        const binArraysYtoX = await dlmmPool.getBinArrayForSwap(false, 50);

        const xToYAddresses = binArraysXtoY.map((b: any) => (b.publicKey as PublicKey).toBase58());
        const yToXAddresses = binArraysYtoX.map((b: any) => (b.publicKey as PublicKey).toBase58());
        // Merged list for backward compatibility
        const uniqueBinArrays = Array.from(new Set([...xToYAddresses, ...yToXAddresses]));

        results.push({
          pool,
          ok: true,
          active_id: activeId,
          bin_arrays: uniqueBinArrays,  // deprecated
          bin_arrays_x_to_y: xToYAddresses,
          bin_arrays_y_to_x: yToXAddresses,
        });

        // Small delay between pools to avoid rate limiting (50ms)
        await new Promise(resolve => setTimeout(resolve, 50));
      } catch (err: any) {
        const message = err && err.message ? String(err.message) : String(err);
        console.warn(`Batch introspect failed for pool ${pool}: ${message}`);
        results.push({
          pool,
          ok: false,
          error: message,
        });
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
    console.error('Error in /meteora/batch_introspect:', message);
    res.status(500).json({ ok: false, error: message });
  }
}
