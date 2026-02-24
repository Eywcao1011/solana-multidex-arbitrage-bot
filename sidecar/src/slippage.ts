import type { Request, Response } from 'express';
import { Connection, PublicKey, Keypair } from '@solana/web3.js';
import {
  LIQUIDITY_STATE_LAYOUT_V4,
  AmmConfigLayout,
  Clmm,
  EXTENSION_TICKARRAY_BITMAP_SIZE,
  FEE_RATE_DENOMINATOR,
  PoolInfoLayout,
  SqrtPriceMath,
  getPdaExBitmapAccount,
} from '@raydium-io/raydium-sdk';
import DLMM from '@meteora-ag/dlmm';
import BN from 'bn.js';
import {
  WhirlpoolContext,
  buildWhirlpoolClient,
  swapQuoteByInputToken,
  ORCA_WHIRLPOOL_PROGRAM_ID,
  PriceMath,
} from '@orca-so/whirlpools-sdk';
import { Percentage } from '@orca-so/common-sdk';
import Decimal from 'decimal.js';
import {
  OnlinePumpAmmSdk,
  type Pool as PumpSwapPool,
  type FeeTier as PumpFeeTier,
  type FeeConfig as PumpFeeConfig,
  type GlobalConfig as PumpGlobalConfig,
} from '@pump-fun/pump-swap-sdk';

const RPC_URL = process.env.RPC_URL || 'https://api.mainnet-beta.solana.com';
const WSOL_MINT = 'So11111111111111111111111111111111111111112';
const DEFAULT_PUBKEY = new PublicKey('11111111111111111111111111111111');
// Pump.fun DLMM uses SDK only; no manual parsing fallback.

let cachedConnection: Connection | null = null;
let cachedOrcaContext: WhirlpoolContext | null = null;
let cachedOrcaClient: ReturnType<typeof buildWhirlpoolClient> | null = null;

function getConnection(): Connection {
  if (cachedConnection) return cachedConnection;
  cachedConnection = new Connection(RPC_URL, 'confirmed');
  return cachedConnection;
}

function getOrcaContext(connection: Connection): WhirlpoolContext {
  if (cachedOrcaContext) return cachedOrcaContext;
  const dummyKeypair = Keypair.generate();
  const dummyWallet = {
    publicKey: dummyKeypair.publicKey,
    signTransaction: async (tx: any) => tx,
    signAllTransactions: async (txs: any[]) => txs,
  };
  cachedOrcaContext = WhirlpoolContext.from(connection, dummyWallet as any);
  return cachedOrcaContext;
}

function getOrcaClient(connection: Connection) {
  if (cachedOrcaClient) return cachedOrcaClient;
  const ctx = getOrcaContext(connection);
  cachedOrcaClient = buildWhirlpoolClient(ctx);
  return cachedOrcaClient;
}

type PoolInfo = {
  address: string;
  dexType: string;
  tokenXMint: string;
  tokenYMint: string;
  reserveXAmount: number;
  reserveYAmount: number;
  decimalsX: number;
  decimalsY: number;
  price: number;
  slot: number;
  feeRate?: number;
  hasWsol: boolean;
  wsolIsY: boolean;
  dlmmPool?: DLMM;
  clmmPoolInfo?: any;
  dammState?: DammState;
};

type PumpFunFees = {
  lpFeeBps: number;
  protocolFeeBps: number;
  coinCreatorFeeBps: number;
  totalFeeBps: number;
};

type PumpFunFeeTier = {
  thresholdLamports: bigint;
  fees: PumpFunFees;
};

type PumpFunFeeConfig = {
  flatFees: PumpFunFees;
  feeTiers: PumpFunFeeTier[];
  protocolFeeRecipient?: PublicKey;
};

function toBaseUnits(amount: number, decimals: number): BN {
  const scale = Math.pow(10, decimals);
  return new BN(Math.round(amount * scale).toString());
}

function fromBaseUnits(amount: BN, decimals: number): number {
  const scale = Math.pow(10, decimals);
  return Number(amount.toString()) / scale;
}

function percentToNumber(value: any): number {
  if (!value) return 0;
  if (typeof value === 'number') return value;
  if (value.numerator && value.denominator) {
    const num = Number(value.numerator.toString());
    const den = Number(value.denominator.toString());
    return den > 0 ? (num / den) * 100 : 0;
  }
  return 0;
}

function sanitizeFeeBps(value: number): number {
  if (!Number.isFinite(value) || value < 0) return 0;
  if (value > 10_000) return 0;
  return Math.floor(value);
}

function buildPumpFunFees(lpFeeBps: number, protocolFeeBps: number, coinCreatorFeeBps: number): PumpFunFees {
  const lp = sanitizeFeeBps(lpFeeBps);
  const protocol = sanitizeFeeBps(protocolFeeBps);
  const creator = sanitizeFeeBps(coinCreatorFeeBps);
  return {
    lpFeeBps: lp,
    protocolFeeBps: protocol,
    coinCreatorFeeBps: creator,
    totalFeeBps: lp + protocol + creator,
  };
}

function pickProtocolFeeRecipient(recipients: PublicKey[]): PublicKey | undefined {
  for (const recipient of recipients) {
    if (!recipient.equals(DEFAULT_PUBKEY)) {
      return recipient;
    }
  }
  return undefined;
}

function convertSdkFeeConfig(globalConfig: PumpGlobalConfig, feeConfig: PumpFeeConfig | null): PumpFunFeeConfig {
  const protocolFeeRecipient = pickProtocolFeeRecipient(globalConfig.protocolFeeRecipients);

  const fallbackFees = buildPumpFunFees(
    Number(globalConfig.lpFeeBasisPoints.toString()),
    Number(globalConfig.protocolFeeBasisPoints.toString()),
    Number(globalConfig.coinCreatorFeeBasisPoints.toString()),
  );

  if (!feeConfig) {
    return { flatFees: fallbackFees, feeTiers: [], protocolFeeRecipient };
  }

  const feeTiers: PumpFunFeeTier[] = feeConfig.feeTiers.map((tier: PumpFeeTier) => ({
    thresholdLamports: BigInt(tier.marketCapLamportsThreshold.toString()),
    fees: buildPumpFunFees(
      Number(tier.fees.lpFeeBps.toString()),
      Number(tier.fees.protocolFeeBps.toString()),
      Number(tier.fees.creatorFeeBps.toString()),
    ),
  }));

  const flatFees = buildPumpFunFees(
    Number(feeConfig.flatFees.lpFeeBps.toString()),
    Number(feeConfig.flatFees.protocolFeeBps.toString()),
    Number(feeConfig.flatFees.creatorFeeBps.toString()),
  );

  return { flatFees, feeTiers, protocolFeeRecipient };
}

function estimateSlippageWithFee(
  reserveBase: number,
  reserveQuote: number,
  tradeSizeQuote: number,
  isBuy: boolean,
  feeRate: number,
): { expectedOut: number; slippagePercent: number; priceImpact: number; feePercent: number } {
  if (reserveBase <= 0 || reserveQuote <= 0) {
    return { expectedOut: 0, slippagePercent: 100, priceImpact: 100, feePercent: 0 };
  }

  const k = reserveBase * reserveQuote;
  const spotPrice = reserveQuote / reserveBase;
  const effectiveFee = feeRate;

  if (isBuy) {
    const amountIn = tradeSizeQuote;
    const amountInAfterFee = amountIn * (1 - effectiveFee);
    const expectedBaseAtSpot = amountIn / spotPrice;

    const newReserveQuote = reserveQuote + amountInAfterFee;
    const newReserveBase = k / newReserveQuote;
    const actualBaseOut = reserveBase - newReserveBase;

    const slippagePercent = ((expectedBaseAtSpot - actualBaseOut) / expectedBaseAtSpot) * 100;
    const effectivePrice = amountIn / actualBaseOut;
    const priceImpact = ((effectivePrice - spotPrice) / spotPrice) * 100;

    return {
      expectedOut: actualBaseOut,
      slippagePercent,
      priceImpact,
      feePercent: effectiveFee * 100,
    };
  }

  const baseIn = tradeSizeQuote / spotPrice;
  const expectedQuote = baseIn * spotPrice;

  const newReserveBase = reserveBase + baseIn;
  const newReserveQuote = k / newReserveBase;
  const grossQuoteOut = reserveQuote - newReserveQuote;
  const actualQuoteOut = grossQuoteOut * (1 - effectiveFee);

  const slippagePercent = ((expectedQuote - actualQuoteOut) / expectedQuote) * 100;
  const effectivePrice = actualQuoteOut / baseIn;
  const priceImpact = ((spotPrice - effectivePrice) / spotPrice) * 100;

  return {
    expectedOut: actualQuoteOut,
    slippagePercent,
    priceImpact,
    feePercent: effectiveFee * 100,
  };
}

async function getMintMeta(connection: Connection, mint: PublicKey): Promise<{ decimals: number; programId: PublicKey }> {
  const info = await connection.getParsedAccountInfo(mint, 'confirmed');
  if (!info.value) {
    throw new Error('mint account not found');
  }
  const parsed = (info.value.data as any)?.parsed;
  const decimals = typeof parsed?.info?.decimals === 'number' ? parsed.info.decimals : 9;
  const owner = info.value.owner as PublicKey;
  return { decimals, programId: owner };
}

async function fetchRaydiumAmmV4Pool(connection: Connection, poolAddress: string): Promise<PoolInfo | null> {
  try {
    const poolPubkey = new PublicKey(poolAddress);
    const accountInfo = await connection.getAccountInfo(poolPubkey, 'confirmed');
    if (!accountInfo || !accountInfo.data) return null;

    const poolState = LIQUIDITY_STATE_LAYOUT_V4.decode(accountInfo.data);

    const baseMint = poolState.baseMint;
    const quoteMint = poolState.quoteMint;
    const baseVault = poolState.baseVault;
    const quoteVault = poolState.quoteVault;

    const baseDecimal = Number(poolState.baseDecimal.toString());
    const quoteDecimal = Number(poolState.quoteDecimal.toString());

    const tradeFeeNumerator = Number(poolState.tradeFeeNumerator.toString());
    const tradeFeeDenominator = Number(poolState.tradeFeeDenominator.toString());
    const feeRate = tradeFeeDenominator > 0 ? tradeFeeNumerator / tradeFeeDenominator : 0;

    const baseNeedTakePnl = Number(poolState.baseNeedTakePnl?.toString() || '0');
    const quoteNeedTakePnl = Number(poolState.quoteNeedTakePnl?.toString() || '0');

    const [baseBalance, quoteBalance, slot] = await Promise.all([
      connection.getTokenAccountBalance(baseVault),
      connection.getTokenAccountBalance(quoteVault),
      connection.getSlot(),
    ]);

    const rawVaultX = parseFloat(baseBalance.value.amount);
    const rawVaultY = parseFloat(quoteBalance.value.amount);

    const availableBaseRaw = Math.max(0, rawVaultX - baseNeedTakePnl);
    const availableQuoteRaw = Math.max(0, rawVaultY - quoteNeedTakePnl);

    const reserveXAmount = availableBaseRaw / Math.pow(10, baseDecimal);
    const reserveYAmount = availableQuoteRaw / Math.pow(10, quoteDecimal);
    const price = reserveXAmount > 0 ? reserveYAmount / reserveXAmount : 0;

    const hasWsol = baseMint.toBase58() === WSOL_MINT || quoteMint.toBase58() === WSOL_MINT;
    const wsolIsY = quoteMint.toBase58() === WSOL_MINT;

    return {
      address: poolAddress,
      dexType: 'Raydium AMM V4',
      tokenXMint: baseMint.toBase58(),
      tokenYMint: quoteMint.toBase58(),
      reserveXAmount,
      reserveYAmount,
      decimalsX: baseDecimal,
      decimalsY: quoteDecimal,
      price,
      slot,
      feeRate,
      hasWsol,
      wsolIsY,
    };
  } catch {
    return null;
  }
}

type RaydiumCpmmDecode = {
  token0Mint: PublicKey;
  token1Mint: PublicKey;
  token0Vault: PublicKey;
  token1Vault: PublicKey;
  mint0Decimals: number;
  mint1Decimals: number;
  ammConfig: PublicKey;
  observationKey: PublicKey;
};

function readPubkey(data: Buffer, offset: number): PublicKey {
  return new PublicKey(data.slice(offset, offset + 32));
}

function decodeRaydiumCpmmPool(data: Buffer): RaydiumCpmmDecode {
  const minSize = 333;
  if (data.length < minSize) {
    throw new Error(`CPMM pool account too short: ${data.length} < ${minSize}`);
  }

  let offset = 8;
  const ammConfig = readPubkey(data, offset);
  offset += 32;
  offset += 32; // pool_creator
  const token0Vault = readPubkey(data, offset);
  offset += 32;
  const token1Vault = readPubkey(data, offset);
  offset += 32;
  offset += 32; // lp_mint
  const token0Mint = readPubkey(data, offset);
  offset += 32;
  const token1Mint = readPubkey(data, offset);
  offset += 32;
  offset += 32; // token_0_program
  offset += 32; // token_1_program
  const observationKey = readPubkey(data, offset);
  offset += 32;

  offset += 1; // auth_bump
  const status = data.readUInt8(offset);
  offset += 1;
  offset += 1; // lp_mint_decimals
  const mint0Decimals = data.readUInt8(offset);
  offset += 1;
  const mint1Decimals = data.readUInt8(offset);

  if ((status & 0b100) !== 0) {
    throw new Error(`CPMM pool swap is disabled (status=${status})`);
  }

  return {
    token0Mint,
    token1Mint,
    token0Vault,
    token1Vault,
    mint0Decimals,
    mint1Decimals,
    ammConfig,
    observationKey,
  };
}

function parseRaydiumCpmmConfig(data: Buffer): { tradeFeeRate: number; protocolFeeRate: number; fundFeeRate: number; creatorFeeRate: number } {
  const minSize = 8 + 1 + 2 + 32 + 4 + 4 + 2 + 4 + 4 + 4;
  if (data.length < minSize) {
    throw new Error(`CPMM amm_config too short: ${data.length} < ${minSize}`);
  }
  let offset = 8;
  offset += 1; // bump
  offset += 2; // index
  offset += 32; // owner
  const protocolFeeRate = data.readUInt32LE(offset);
  offset += 4;
  const tradeFeeRate = data.readUInt32LE(offset);
  offset += 4;
  offset += 2; // tick spacing
  const fundFeeRate = data.readUInt32LE(offset);
  offset += 4;
  const creatorFeeRate = data.readUInt32LE(offset);
  return { tradeFeeRate, protocolFeeRate, fundFeeRate, creatorFeeRate };
}

async function fetchRaydiumCpmmPool(connection: Connection, poolAddress: string): Promise<PoolInfo | null> {
  try {
    const poolPubkey = new PublicKey(poolAddress);
    const accountInfo = await connection.getAccountInfo(poolPubkey, 'confirmed');
    if (!accountInfo || !accountInfo.data) return null;

    const decoded = decodeRaydiumCpmmPool(accountInfo.data);
    const [vault0Balance, vault1Balance, slot, ammConfigInfo] = await Promise.all([
      connection.getTokenAccountBalance(decoded.token0Vault),
      connection.getTokenAccountBalance(decoded.token1Vault),
      connection.getSlot(),
      connection.getAccountInfo(decoded.ammConfig, 'confirmed'),
    ]);

    const reserve0 = parseFloat(vault0Balance.value.amount) / Math.pow(10, decoded.mint0Decimals);
    const reserve1 = parseFloat(vault1Balance.value.amount) / Math.pow(10, decoded.mint1Decimals);
    const price = reserve0 > 0 ? reserve1 / reserve0 : 0;

    const hasWsol = decoded.token0Mint.toBase58() === WSOL_MINT || decoded.token1Mint.toBase58() === WSOL_MINT;
    const wsolIsY = decoded.token1Mint.toBase58() === WSOL_MINT;

    const denom = 1_000_000;
    let feeRate = 0.0025;
    if (ammConfigInfo?.data) {
      try {
        const fees = parseRaydiumCpmmConfig(ammConfigInfo.data);
        if (fees.tradeFeeRate <= denom) {
          feeRate = fees.tradeFeeRate / denom;
        }
      } catch {
        // fallback
      }
    }

    return {
      address: poolAddress,
      dexType: 'Raydium CPMM',
      tokenXMint: decoded.token0Mint.toBase58(),
      tokenYMint: decoded.token1Mint.toBase58(),
      reserveXAmount: reserve0,
      reserveYAmount: reserve1,
      decimalsX: decoded.mint0Decimals,
      decimalsY: decoded.mint1Decimals,
      price,
      slot,
      feeRate,
      hasWsol,
      wsolIsY,
    };
  } catch {
    return null;
  }
}

function buildEmptyExBitmap(poolId: PublicKey) {
  const makeBitmap = () =>
    Array.from({ length: EXTENSION_TICKARRAY_BITMAP_SIZE }, () =>
      Array.from({ length: 8 }, () => new BN(0))
    );
  return {
    poolId,
    positiveTickArrayBitmap: makeBitmap(),
    negativeTickArrayBitmap: makeBitmap(),
  };
}

async function fetchRaydiumClmmPool(connection: Connection, poolAddress: string): Promise<PoolInfo | null> {
  try {
    const poolPubkey = new PublicKey(poolAddress);
    const accountInfo = await connection.getAccountInfo(poolPubkey, 'confirmed');
    if (!accountInfo || !accountInfo.data) return null;

    const poolState = PoolInfoLayout.decode(accountInfo.data);
    const programId = accountInfo.owner as PublicKey;

    const mintA = poolState.mintA as PublicKey;
    const mintB = poolState.mintB as PublicKey;
    const vaultA = poolState.vaultA as PublicKey;
    const vaultB = poolState.vaultB as PublicKey;
    const ammConfigId = poolState.ammConfig as PublicKey;

    const [mintAMeta, mintBMeta, ammConfigAccount, slot] = await Promise.all([
      getMintMeta(connection, mintA),
      getMintMeta(connection, mintB),
      connection.getAccountInfo(ammConfigId, 'confirmed'),
      connection.getSlot(),
    ]);

    if (!ammConfigAccount || !ammConfigAccount.data) {
      return null;
    }

    const ammConfig = AmmConfigLayout.decode(ammConfigAccount.data);
    const tradeFeeRateRaw = Number(ammConfig.tradeFeeRate?.toString?.() || 0);
    const feeRate = tradeFeeRateRaw / Number(FEE_RATE_DENOMINATOR.toString());

    const decimalsA = Number(poolState.mintDecimalsA);
    const decimalsB = Number(poolState.mintDecimalsB);

    let reserveAAmount = 0;
    let reserveBAmount = 0;
    try {
      const vaultABalance = await connection.getTokenAccountBalance(vaultA);
      reserveAAmount = parseFloat(vaultABalance.value.amount) / Math.pow(10, decimalsA);
    } catch { }
    try {
      const vaultBBalance = await connection.getTokenAccountBalance(vaultB);
      reserveBAmount = parseFloat(vaultBBalance.value.amount) / Math.pow(10, decimalsB);
    } catch { }

    const currentPrice = SqrtPriceMath.sqrtPriceX64ToPrice(
      poolState.sqrtPriceX64 as BN,
      decimalsA,
      decimalsB,
    );
    const price = parseFloat(currentPrice.toString());

    const exBitmapAddress = getPdaExBitmapAccount(programId, poolPubkey).publicKey;
    let exBitmapInfo = buildEmptyExBitmap(poolPubkey);
    try {
      const exBitmapMap = await Clmm.fetchExBitmaps({
        connection,
        exBitmapAddress: [exBitmapAddress],
        batchRequest: true,
      });
      const fetched = exBitmapMap[exBitmapAddress.toString()];
      if (fetched) {
        exBitmapInfo = fetched as any;
      }
    } catch { }

    const poolInfo: any = {
      id: poolPubkey,
      mintA: {
        programId: mintAMeta.programId,
        mint: mintA,
        vault: vaultA,
        decimals: decimalsA,
      },
      mintB: {
        programId: mintBMeta.programId,
        mint: mintB,
        vault: vaultB,
        decimals: decimalsB,
      },
      ammConfig: {
        id: ammConfigId,
        index: Number(ammConfig.index ?? 0),
        protocolFeeRate: Number(ammConfig.protocolFeeRate?.toString?.() || 0),
        tradeFeeRate: tradeFeeRateRaw,
        tickSpacing: Number(ammConfig.tickSpacing ?? poolState.tickSpacing),
        fundFeeRate: Number(ammConfig.fundFeeRate?.toString?.() || 0),
        fundOwner: ((ammConfig.fundOwner as PublicKey) || DEFAULT_PUBKEY).toBase58(),
        description: '',
      },
      observationId: poolState.observationId,
      creator: poolState.creator,
      programId,
      version: 6,
      tickSpacing: poolState.tickSpacing,
      liquidity: poolState.liquidity,
      sqrtPriceX64: poolState.sqrtPriceX64,
      currentPrice,
      tickCurrent: poolState.tickCurrent,
      observationIndex: poolState.observationIndex,
      observationUpdateDuration: poolState.observationUpdateDuration,
      feeGrowthGlobalX64A: poolState.feeGrowthGlobalX64A,
      feeGrowthGlobalX64B: poolState.feeGrowthGlobalX64B,
      protocolFeesTokenA: poolState.protocolFeesTokenA,
      protocolFeesTokenB: poolState.protocolFeesTokenB,
      swapInAmountTokenA: poolState.swapInAmountTokenA,
      swapOutAmountTokenB: poolState.swapOutAmountTokenB,
      swapInAmountTokenB: poolState.swapInAmountTokenB,
      swapOutAmountTokenA: poolState.swapOutAmountTokenA,
      tickArrayBitmap: poolState.tickArrayBitmap,
      rewardInfos: poolState.rewardInfos,
      day: {} as any,
      week: {} as any,
      month: {} as any,
      tvl: 0,
      lookupTableAccount: DEFAULT_PUBKEY,
      startTime: Number(poolState.startTime ?? 0),
      exBitmapInfo,
    };

    const hasWsol = mintA.toBase58() === WSOL_MINT || mintB.toBase58() === WSOL_MINT;
    const wsolIsY = mintB.toBase58() === WSOL_MINT;

    return {
      address: poolAddress,
      dexType: 'Raydium CLMM',
      tokenXMint: mintA.toBase58(),
      tokenYMint: mintB.toBase58(),
      reserveXAmount: reserveAAmount,
      reserveYAmount: reserveBAmount,
      decimalsX: decimalsA,
      decimalsY: decimalsB,
      price,
      slot,
      feeRate,
      hasWsol,
      wsolIsY,
      clmmPoolInfo: poolInfo,
    };
  } catch {
    return null;
  }
}

async function quoteRaydiumClmmSwap(
  connection: Connection,
  poolInfo: any,
  amountIn: number,
  baseMint: PublicKey,
  inDecimals: number,
  outDecimals: number,
): Promise<{ outAmount: number; minOutAmount: number; priceImpactPercent: number; feePercent: number }> {
  const amountInLamports = toBaseUnits(amountIn, inDecimals);
  const tickArrayCacheMap = await Clmm.fetchMultiplePoolTickArrays({
    connection,
    poolKeys: [poolInfo],
    batchRequest: true,
  });
  const tickArrayCache = tickArrayCacheMap[poolInfo.id.toString()] || {};
  const result = await Clmm.computeAmountOutAndCheckToken({
    connection,
    poolInfo,
    tickArrayCache,
    baseMint,
    amountIn: amountInLamports,
    slippage: 0.0001,
    catchLiquidityInsufficient: true,
  });

  const outAmount = fromBaseUnits(result.amountOut.amount, outDecimals);
  const minOutAmount = fromBaseUnits(result.minAmountOut.amount, outDecimals);
  const feePercent = amountInLamports.isZero()
    ? 0
    : (Number(result.fee.toString()) / Number(amountInLamports.toString())) * 100;
  const priceImpactPercent = percentToNumber(result.priceImpact);

  return { outAmount, minOutAmount, priceImpactPercent, feePercent };
}

async function fetchOrcaWhirlpoolPool(connection: Connection, poolAddress: string): Promise<PoolInfo | null> {
  try {
    const poolPubkey = new PublicKey(poolAddress);
    const client = getOrcaClient(connection);
    const whirlpool = await client.getPool(poolPubkey);
    const poolData = whirlpool.getData();

    const tokenAInfo = whirlpool.getTokenAInfo();
    const tokenBInfo = whirlpool.getTokenBInfo();
    const vaultAInfo = whirlpool.getTokenVaultAInfo();
    const vaultBInfo = whirlpool.getTokenVaultBInfo();

    const decimalsA = tokenAInfo.decimals;
    const decimalsB = tokenBInfo.decimals;
    const reserveAAmount = Number(vaultAInfo.amount.toString()) / Math.pow(10, decimalsA);
    const reserveBAmount = Number(vaultBInfo.amount.toString()) / Math.pow(10, decimalsB);

    const priceDecimal = PriceMath.sqrtPriceX64ToPrice(poolData.sqrtPrice, decimalsA, decimalsB);
    const price = Number(priceDecimal.toString());

    const mintAStr = tokenAInfo.mint.toBase58();
    const mintBStr = tokenBInfo.mint.toBase58();
    const hasWsol = mintAStr === WSOL_MINT || mintBStr === WSOL_MINT;
    const wsolIsY = mintBStr === WSOL_MINT;

    return {
      address: poolAddress,
      dexType: 'Orca Whirlpool',
      tokenXMint: mintAStr,
      tokenYMint: mintBStr,
      reserveXAmount: reserveAAmount,
      reserveYAmount: reserveBAmount,
      decimalsX: decimalsA,
      decimalsY: decimalsB,
      price,
      slot: await connection.getSlot(),
      feeRate: poolData.feeRate / 1_000_000,
      hasWsol,
      wsolIsY,
    };
  } catch {
    return null;
  }
}

async function quoteOrcaWhirlpoolSwap(
  connection: Connection,
  poolAddress: string,
  tokenMintIn: PublicKey,
  amountIn: number,
  inDecimals: number,
  outDecimals: number,
): Promise<{ outAmount: number; minOutAmount: number }> {
  const ctx = getOrcaContext(connection);
  const client = getOrcaClient(connection);
  const whirlpool = await client.getPool(poolAddress);
  const tokenAmount = new BN(toBaseUnits(amountIn, inDecimals).toString());
  const slippage = Percentage.fromFraction(1, 1000);
  const quote = await swapQuoteByInputToken(
    whirlpool,
    tokenMintIn,
    tokenAmount,
    slippage,
    ORCA_WHIRLPOOL_PROGRAM_ID,
    ctx.fetcher,
  );

  const outAmount = Number(quote.estimatedAmountOut.toString()) / Math.pow(10, outDecimals);
  const minOutAmount = Number(quote.otherAmountThreshold.toString()) / Math.pow(10, outDecimals);
  return { outAmount, minOutAmount };
}

async function fetchMeteoraDlmmPool(connection: Connection, poolAddress: string): Promise<PoolInfo | null> {
  try {
    const poolPubkey = new PublicKey(poolAddress);
    const dlmmPool = await DLMM.create(connection, poolPubkey);

    const tokenXMint: PublicKey = dlmmPool.lbPair.tokenXMint;
    const tokenYMint: PublicKey = dlmmPool.lbPair.tokenYMint;
    const reserveX: PublicKey = dlmmPool.lbPair.reserveX;
    const reserveY: PublicKey = dlmmPool.lbPair.reserveY;
    const binStep: number = dlmmPool.lbPair.binStep;

    const [metaX, metaY, balanceX, balanceY, slot] = await Promise.all([
      getMintMeta(connection, tokenXMint),
      getMintMeta(connection, tokenYMint),
      connection.getTokenAccountBalance(reserveX),
      connection.getTokenAccountBalance(reserveY),
      connection.getSlot(),
    ]);

    const reserveXAmount = parseFloat(balanceX.value.amount) / Math.pow(10, metaX.decimals);
    const reserveYAmount = parseFloat(balanceY.value.amount) / Math.pow(10, metaY.decimals);
    let price = 0;
    try {
      const activeBin = await dlmmPool.getActiveBin();
      const priceStr = dlmmPool.fromPricePerLamport(Number(activeBin.price));
      price = parseFloat(priceStr);
    } catch {
      price = 0;
    }

    const hasWsol = tokenXMint.toBase58() === WSOL_MINT || tokenYMint.toBase58() === WSOL_MINT;
    const wsolIsY = tokenYMint.toBase58() === WSOL_MINT;

    return {
      address: poolAddress,
      dexType: 'Meteora DLMM',
      tokenXMint: tokenXMint.toBase58(),
      tokenYMint: tokenYMint.toBase58(),
      reserveXAmount,
      reserveYAmount,
      decimalsX: metaX.decimals,
      decimalsY: metaY.decimals,
      price,
      slot,
      feeRate: 0,
      hasWsol,
      wsolIsY,
      dlmmPool,
    };
  } catch {
    return null;
  }
}

async function quoteDlmmSwap(
  dlmmPool: DLMM,
  amountIn: number,
  swapForY: boolean,
  inDecimals: number,
  outDecimals: number,
  spotPrice: number,
): Promise<{ outAmount: number; slippagePercent: number; priceImpactPercent: number; feePercent: number }> {
  const binArrays = await dlmmPool.getBinArrayForSwap(swapForY);
  const amountInLamports = toBaseUnits(amountIn, inDecimals);
  const allowedSlippageBps = new BN(1);
  const quote = await dlmmPool.swapQuote(amountInLamports, swapForY, allowedSlippageBps, binArrays);

  const outAmount = fromBaseUnits(quote.outAmount, outDecimals);
  const consumedIn = fromBaseUnits(quote.consumedInAmount, inDecimals);
  const feeAmount = fromBaseUnits(quote.fee, inDecimals);
  const protocolFeeAmount = fromBaseUnits(quote.protocolFee, inDecimals);

  const expectedOut = spotPrice > 0
    ? (swapForY ? amountIn * spotPrice : amountIn / spotPrice)
    : 0;
  const slippagePercent = expectedOut > 0
    ? ((expectedOut - outAmount) / expectedOut) * 100
    : 0;
  const priceImpactPercent = Number(quote.priceImpact.toString());
  const feePercent = consumedIn > 0
    ? ((feeAmount + protocolFeeAmount) / consumedIn) * 100
    : 0;

  return {
    outAmount,
    slippagePercent,
    priceImpactPercent,
    feePercent,
  };
}

// ===== Meteora DAMM V2 =====
const DAMM_FEE_DENOMINATOR = 1_000_000_000;
const DAMM_Q64 = 18_446_744_073_709_551_616.0;
const DAMM_EPSILON = 1e-12;
const DAMM_MAX_FEE_RATIO = 0.95;
const Q64 = Math.pow(2, 64);

type DammFeeInfo = {
  baseFeeRatio: number;
  lpBaseFeeRatio: number;
  protocolFeeRatio: number;
  dynamicFeeRatio: number;
};

type DammDynamicFee = {
  binStep: number;
  binStepQ64: bigint;
  maxVolatilityAccumulator: number;
  variableFeeControl: number;
  volatilityAccumulator: bigint;
};

type DammState = {
  sqrtPrice: number;
  sqrtMinPrice: number;
  sqrtMaxPrice: number;
  liquidity: number;
  totalFeeRatio: number;
};

type DammSwapContext = {
  sqrtPrice: number;
  sqrtMinPrice: number;
  sqrtMaxPrice: number;
  liquidity: number;
};

type DammSwapStep = {
  tokenIn: number;
  tokenOut: number;
};

function readU8(data: Buffer, offset: number): number {
  return data.readUInt8(offset);
}

function readU16LE(data: Buffer, offset: number): number {
  return data.readUInt16LE(offset);
}

function readU32LE(data: Buffer, offset: number): number {
  return data.readUInt32LE(offset);
}

function readU64LE(data: Buffer, offset: number): bigint {
  return data.readBigUInt64LE(offset);
}

function readU128LE(data: Buffer, offset: number): bigint {
  const low = data.readBigUInt64LE(offset);
  const high = data.readBigUInt64LE(offset + 8);
  return (high << 64n) + low;
}

function computeDammDynamicFeeRatio(dynamic: DammDynamicFee, baseFeeRatio: number): number {
  if (dynamic.variableFeeControl === 0 || dynamic.binStep === 0) {
    return 0;
  }

  let va = Number(dynamic.volatilityAccumulator);
  if (!Number.isFinite(va) || va <= 0) {
    va = 0;
  }

  const maxAcc = dynamic.maxVolatilityAccumulator;
  if (Number.isFinite(maxAcc) && maxAcc > 0) {
    va = Math.min(va, maxAcc);
  }
  if (va <= 0) {
    return 0;
  }

  let step = 0;
  if (dynamic.binStepQ64 !== 0n) {
    step = Number(dynamic.binStepQ64) / Q64 - 1.0;
  }
  if (!(Number.isFinite(step) && step > 0)) {
    step = dynamic.binStep / 10_000.0;
  }
  if (!(Number.isFinite(step) && step > 0)) {
    return 0;
  }

  const c = dynamic.variableFeeControl;
  let ratio = ((va * step) ** 2 * c) / 100_000_000_000.0;
  if (!Number.isFinite(ratio) || ratio < 0) {
    ratio = 0;
  }

  const maxRatio = Math.max(baseFeeRatio * 0.2, 0.0);
  return Math.min(ratio, maxRatio);
}

function decodeDammDynamicFee(buffer: Buffer, offset: number): { fee: DammDynamicFee | null; offset: number } {
  const initialized = readU8(buffer, offset) !== 0;
  offset += 1;
  offset += 7;
  const maxVolatilityAccumulator = readU32LE(buffer, offset);
  offset += 4;
  const variableFeeControl = readU32LE(buffer, offset);
  offset += 4;
  const binStep = readU16LE(buffer, offset);
  offset += 2;
  offset += 2; // filter_period
  offset += 2; // decay_period
  offset += 2; // reduction_factor
  offset += 8; // last_update_timestamp
  const binStepQ64 = readU128LE(buffer, offset);
  offset += 16;
  offset += 16; // sqrt_price_reference
  const volatilityAccumulator = readU128LE(buffer, offset);
  offset += 16;
  offset += 16; // volatility_reference

  if (!initialized) {
    return { fee: null, offset };
  }

  return {
    fee: {
      binStep,
      binStepQ64,
      maxVolatilityAccumulator,
      variableFeeControl,
      volatilityAccumulator,
    },
    offset,
  };
}

function decodeDammPoolFees(buffer: Buffer, offset: number): { feeInfo: DammFeeInfo; offset: number } {
  const baseFeeNumerator = Number(readU64LE(buffer, offset));
  offset += 8;
  offset += 1; // fee_scheduler_mode
  offset += 5; // padding
  offset += 2; // number_of_period
  offset += 8; // period_frequency
  offset += 8; // reduction_factor
  offset += 8; // padding_1

  const protocolFeePercent = readU8(buffer, offset);
  offset += 1;
  offset += 1; // partner_fee_percent
  const referralFeePercent = readU8(buffer, offset);
  offset += 1;
  offset += 5; // padding_0

  const dynamicResult = decodeDammDynamicFee(buffer, offset);
  offset = dynamicResult.offset;
  offset += 16; // padding_1

  let baseFeeRatio = baseFeeNumerator / DAMM_FEE_DENOMINATOR;
  if (!Number.isFinite(baseFeeRatio) || baseFeeRatio < 0) {
    baseFeeRatio = 0;
  }

  let protocolFeeRatio = baseFeeRatio * Math.min(Math.max(protocolFeePercent / 100.0, 0), 1);
  const referralRatio = protocolFeeRatio * Math.min(Math.max(referralFeePercent / 100.0, 0), 1);
  protocolFeeRatio = Math.max(protocolFeeRatio - referralRatio, 0);
  const lpBaseFeeRatio = Math.max(baseFeeRatio - protocolFeeRatio, 0);

  const dynamicFeeRatio = dynamicResult.fee
    ? computeDammDynamicFeeRatio(dynamicResult.fee, baseFeeRatio)
    : 0.0;

  return {
    feeInfo: {
      baseFeeRatio,
      lpBaseFeeRatio,
      protocolFeeRatio,
      dynamicFeeRatio,
    },
    offset,
  };
}

function buildDammState(
  feeInfo: DammFeeInfo,
  liquidityRaw: bigint,
  sqrtPriceX64: bigint,
  sqrtMinPriceX64: bigint,
  sqrtMaxPriceX64: bigint,
  maxFeeRatio: number,
): DammState | null {
  const liquidity = Number(liquidityRaw) / DAMM_Q64;
  if (!Number.isFinite(liquidity) || liquidity <= 0) {
    return null;
  }

  const sqrtPrice = Number(sqrtPriceX64) / DAMM_Q64;
  const sqrtMinPrice = Number(sqrtMinPriceX64) / DAMM_Q64;
  const sqrtMaxPrice = Number(sqrtMaxPriceX64) / DAMM_Q64;
  if (!(Number.isFinite(sqrtPrice) && Number.isFinite(sqrtMinPrice) && Number.isFinite(sqrtMaxPrice))) {
    return null;
  }
  if (!(sqrtMaxPrice > sqrtMinPrice && sqrtPrice >= sqrtMinPrice - DAMM_EPSILON && sqrtPrice <= sqrtMaxPrice + DAMM_EPSILON)) {
    return null;
  }

  const rawFee = Math.max(feeInfo.baseFeeRatio, 0) + Math.max(feeInfo.dynamicFeeRatio, 0);
  const feeCap = Math.min(DAMM_MAX_FEE_RATIO, maxFeeRatio);
  const totalFeeRatio = Math.min(Math.max(rawFee, 0), feeCap);

  return {
    sqrtPrice,
    sqrtMinPrice,
    sqrtMaxPrice,
    liquidity,
    totalFeeRatio,
  };
}

function dammSwapContext(state: DammState): DammSwapContext | null {
  if (!(state.liquidity > 0 && Number.isFinite(state.liquidity))) {
    return null;
  }
  if (!(state.sqrtMinPrice > 0 && state.sqrtMaxPrice > state.sqrtMinPrice)) {
    return null;
  }
  return {
    sqrtPrice: state.sqrtPrice,
    sqrtMinPrice: state.sqrtMinPrice,
    sqrtMaxPrice: state.sqrtMaxPrice,
    liquidity: state.liquidity,
  };
}

function dammAvailableTokenA(ctx: DammSwapContext): number {
  const value = ctx.liquidity * (1 / ctx.sqrtPrice - 1 / ctx.sqrtMaxPrice);
  return Number.isFinite(value) && value > 0 ? value : 0;
}

function dammAvailableTokenB(ctx: DammSwapContext): number {
  const value = ctx.liquidity * (ctx.sqrtPrice - ctx.sqrtMinPrice);
  return Number.isFinite(value) && value > 0 ? value : 0;
}

function dammSwapTokenAOut(ctx: DammSwapContext, amountOut: number): DammSwapStep | null {
  if (amountOut <= 0) {
    return { tokenIn: 0, tokenOut: 0 };
  }
  if (amountOut > dammAvailableTokenA(ctx) + DAMM_EPSILON) {
    return null;
  }
  const invOld = 1 / ctx.sqrtPrice;
  const invNew = invOld - amountOut / ctx.liquidity;
  if (invNew <= 0) {
    return null;
  }
  const sqrtNew = 1 / invNew;
  if (sqrtNew > ctx.sqrtMaxPrice + DAMM_EPSILON) {
    return null;
  }
  const tokenIn = ctx.liquidity * (sqrtNew - ctx.sqrtPrice);
  if (!(Number.isFinite(tokenIn) && tokenIn > 0)) {
    return null;
  }
  return { tokenIn, tokenOut: amountOut };
}

function dammSwapTokenAIn(ctx: DammSwapContext, amountIn: number): DammSwapStep | null {
  if (amountIn <= 0) {
    return { tokenIn: 0, tokenOut: 0 };
  }
  const invOld = 1 / ctx.sqrtPrice;
  const invNew = invOld + amountIn / ctx.liquidity;
  if (invNew <= 0) {
    return null;
  }
  const sqrtNew = 1 / invNew;
  if (sqrtNew < ctx.sqrtMinPrice - DAMM_EPSILON) {
    return null;
  }
  const tokenOut = ctx.liquidity * (ctx.sqrtPrice - sqrtNew);
  if (!(Number.isFinite(tokenOut) && tokenOut > 0)) {
    return null;
  }
  return { tokenIn: amountIn, tokenOut };
}

function dammSwapTokenBOut(ctx: DammSwapContext, amountOut: number): DammSwapStep | null {
  if (amountOut <= 0) {
    return { tokenIn: 0, tokenOut: 0 };
  }
  if (amountOut > dammAvailableTokenB(ctx) + DAMM_EPSILON) {
    return null;
  }
  const sqrtNew = ctx.sqrtPrice - amountOut / ctx.liquidity;
  if (sqrtNew < ctx.sqrtMinPrice - DAMM_EPSILON) {
    return null;
  }
  const tokenIn = ctx.liquidity * (1 / sqrtNew - 1 / ctx.sqrtPrice);
  if (!(Number.isFinite(tokenIn) && tokenIn > 0)) {
    return null;
  }
  return { tokenIn, tokenOut: amountOut };
}

function dammSwapTokenBIn(ctx: DammSwapContext, amountIn: number): DammSwapStep | null {
  if (amountIn <= 0) {
    return { tokenIn: 0, tokenOut: 0 };
  }
  const sqrtNew = ctx.sqrtPrice + amountIn / ctx.liquidity;
  if (sqrtNew > ctx.sqrtMaxPrice + DAMM_EPSILON) {
    return null;
  }
  const tokenOut = ctx.liquidity * (1 / ctx.sqrtPrice - 1 / sqrtNew);
  if (!(Number.isFinite(tokenOut) && tokenOut > 0)) {
    return null;
  }
  return { tokenIn: amountIn, tokenOut };
}

function toRawFloat(amount: number, decimals: number): number {
  if (!Number.isFinite(amount)) return 0;
  return amount * Math.pow(10, decimals);
}

function q64ToPrice(sqrtPriceX64: bigint, decimalsBase: number, decimalsQuote: number): number {
  if (sqrtPriceX64 === 0n) return 0;
  const ratio = Number(sqrtPriceX64) / DAMM_Q64;
  if (!Number.isFinite(ratio) || ratio <= 0) return 0;
  let price = ratio * ratio;
  const decimalsAdjust = decimalsBase - decimalsQuote;
  if (decimalsAdjust !== 0) {
    price *= Math.pow(10, decimalsAdjust);
  }
  return Number.isFinite(price) && price > 0 ? price : 0;
}

function spotQuotePerBase(price: number, wsolIsY: boolean): number {
  if (price <= 0) return 0;
  return wsolIsY ? price : 1 / price;
}

function dammQuoteExactIn(
  state: DammState,
  amountInUnits: number,
  inputIsTokenA: boolean,
  inDecimals: number,
  outDecimals: number,
): { outAmount: number; feePercent: number } | null {
  const ctx = dammSwapContext(state);
  if (!ctx) return null;
  const feeRatio = Math.min(Math.max(state.totalFeeRatio, 0), DAMM_MAX_FEE_RATIO);
  if (!(feeRatio >= 0 && feeRatio < 1)) return null;

  const amountInRaw = toRawFloat(amountInUnits, inDecimals);
  if (!(amountInRaw > 0)) return null;
  const netInRaw = amountInRaw * (1 - feeRatio);
  if (!(netInRaw > 0)) return null;

  const step = inputIsTokenA
    ? dammSwapTokenAIn(ctx, netInRaw)
    : dammSwapTokenBIn(ctx, netInRaw);
  if (!step) return null;

  const outAmount = step.tokenOut / Math.pow(10, outDecimals);
  if (!Number.isFinite(outAmount) || outAmount <= 0) return null;

  return { outAmount, feePercent: feeRatio * 100 };
}

async function fetchMeteoraDammV2Pool(connection: Connection, poolAddress: string): Promise<PoolInfo | null> {
  try {
    const poolPubkey = new PublicKey(poolAddress);
    const accountInfo = await connection.getAccountInfo(poolPubkey, 'confirmed');
    if (!accountInfo || !accountInfo.data) return null;

    const data = accountInfo.data;
    if (data.length < 360) {
      return null;
    }

    let offset = 8;
    const decodedFees = decodeDammPoolFees(data, offset);
    offset = decodedFees.offset;

    const tokenAMint = readPubkey(data, offset); offset += 32;
    const tokenBMint = readPubkey(data, offset); offset += 32;
    const tokenAVault = readPubkey(data, offset); offset += 32;
    const tokenBVault = readPubkey(data, offset); offset += 32;
    offset += 32; // whitelisted vault
    offset += 32; // partner
    const liquidityRaw = readU128LE(data, offset); offset += 16;
    offset += 16; // padding
    offset += 8 * 4; // protocol/partner fees
    const sqrtMinPriceX64 = readU128LE(data, offset); offset += 16;
    const sqrtMaxPriceX64 = readU128LE(data, offset); offset += 16;
    const sqrtPriceX64 = readU128LE(data, offset); offset += 16;
    offset += 8; // activation point
    offset += 1; // activation type
    offset += 1; // pool status
    offset += 1; // token a flag
    offset += 1; // token b flag
    offset += 1; // collect fee mode
    offset += 1; // pool type
    offset += 2; // padding

    const [mintAMeta, mintBMeta, vaultABalance, vaultBBalance, slot] = await Promise.all([
      getMintMeta(connection, tokenAMint),
      getMintMeta(connection, tokenBMint),
      connection.getTokenAccountBalance(tokenAVault),
      connection.getTokenAccountBalance(tokenBVault),
      connection.getSlot(),
    ]);

    const reserveA = parseFloat(vaultABalance.value.amount) / Math.pow(10, mintAMeta.decimals);
    const reserveB = parseFloat(vaultBBalance.value.amount) / Math.pow(10, mintBMeta.decimals);
    let price = q64ToPrice(sqrtPriceX64, mintAMeta.decimals, mintBMeta.decimals);
    if (!(price > 0)) {
      price = reserveA > 0 ? reserveB / reserveA : 0;
    }

    const hasWsol = tokenAMint.toBase58() === WSOL_MINT || tokenBMint.toBase58() === WSOL_MINT;
    const wsolIsY = tokenBMint.toBase58() === WSOL_MINT;

    const maxFeeRatio = DAMM_MAX_FEE_RATIO;
    const dammState = buildDammState(
      decodedFees.feeInfo,
      liquidityRaw,
      sqrtPriceX64,
      sqrtMinPriceX64,
      sqrtMaxPriceX64,
      maxFeeRatio,
    );
    const rawFeeRate = decodedFees.feeInfo.baseFeeRatio + decodedFees.feeInfo.dynamicFeeRatio;
    const feeRate = dammState ? dammState.totalFeeRatio : Math.min(Math.max(rawFeeRate, 0), maxFeeRatio);

    return {
      address: poolAddress,
      dexType: 'Meteora DAMM V2',
      tokenXMint: tokenAMint.toBase58(),
      tokenYMint: tokenBMint.toBase58(),
      reserveXAmount: reserveA,
      reserveYAmount: reserveB,
      decimalsX: mintAMeta.decimals,
      decimalsY: mintBMeta.decimals,
      price,
      slot,
      feeRate,
      hasWsol,
      wsolIsY,
      dammState: dammState || undefined,
    };
  } catch {
    return null;
  }
}


function selectPumpFunFees(config: PumpFunFeeConfig, marketCapLamports?: bigint | null): PumpFunFees {
  if (!config.feeTiers.length || !marketCapLamports) {
    return config.flatFees;
  }

  let selected = config.flatFees;
  for (const tier of config.feeTiers) {
    if (marketCapLamports >= tier.thresholdLamports) {
      selected = tier.fees;
    }
  }
  return selected;
}

function computeMarketCapLamports(
  lpSupplyRaw: bigint | undefined,
  baseDecimals: number,
  price: number,
  quoteDecimals: number,
): bigint | null {
  if (!lpSupplyRaw || lpSupplyRaw <= 0n) return null;
  if (!(Number.isFinite(price) && price > 0)) return null;
  const supply = new Decimal(lpSupplyRaw.toString()).div(new Decimal(10).pow(baseDecimals));
  if (!supply.isFinite() || supply.lte(0)) return null;
  const marketCapQuote = supply.mul(new Decimal(price));
  const marketCapLamports = marketCapQuote.mul(new Decimal(10).pow(quoteDecimals));
  if (!marketCapLamports.isFinite() || marketCapLamports.lte(0)) return null;
  return BigInt(marketCapLamports.floor().toFixed(0));
}

async function fetchPumpFunDlmmFees(connection: Connection): Promise<PumpFunFeeConfig> {
  const sdk = new OnlinePumpAmmSdk(connection);
  const [globalConfig, feeConfig] = await Promise.all([
    sdk.fetchGlobalConfigAccount(),
    sdk.fetchFeeConfigAccount().catch(() => null),
  ]);
  return convertSdkFeeConfig(globalConfig, feeConfig);
}

async function fetchPumpFunDlmmPool(connection: Connection, poolAddress: string): Promise<PoolInfo | null> {
  const poolPubkey = new PublicKey(poolAddress);
  const sdk = new OnlinePumpAmmSdk(connection);

  const pool = await sdk.fetchPool(poolPubkey);

  const [baseMeta, quoteMeta, baseBalance, quoteBalance, slot, feeConfig] = await Promise.all([
    getMintMeta(connection, pool.baseMint),
    getMintMeta(connection, pool.quoteMint),
    connection.getTokenAccountBalance(pool.poolBaseTokenAccount),
    connection.getTokenAccountBalance(pool.poolQuoteTokenAccount),
    connection.getSlot(),
    fetchPumpFunDlmmFees(connection),
  ]);

  const reserveBase = parseFloat(baseBalance.value.amount) / Math.pow(10, baseMeta.decimals);
  const reserveQuote = parseFloat(quoteBalance.value.amount) / Math.pow(10, quoteMeta.decimals);
  const price = reserveBase > 0 ? reserveQuote / reserveBase : 0;

  const hasWsol = pool.baseMint.toBase58() === WSOL_MINT || pool.quoteMint.toBase58() === WSOL_MINT;
  const wsolIsY = pool.quoteMint.toBase58() === WSOL_MINT;
  const marketCapLamports = computeMarketCapLamports(
    BigInt(pool.lpSupply.toString()),
    baseMeta.decimals,
    price,
    quoteMeta.decimals,
  );
  const appliedFees = selectPumpFunFees(feeConfig, marketCapLamports);
  const feeRate = appliedFees.totalFeeBps / 10_000;

  return {
    address: poolAddress,
    dexType: 'Pump.fun DLMM',
    tokenXMint: pool.baseMint.toBase58(),
    tokenYMint: pool.quoteMint.toBase58(),
    reserveXAmount: reserveBase,
    reserveYAmount: reserveQuote,
    decimalsX: baseMeta.decimals,
    decimalsY: quoteMeta.decimals,
    price,
    slot,
    feeRate,
    hasWsol,
    wsolIsY,
  };
}

type SlippageRequest = {
  pool?: string;
  dex?: string;
  trade_size_quote?: number;
};

export async function quotePoolSlippage(req: Request, res: Response): Promise<void> {
  try {
    const body = req.body as SlippageRequest;
    if (!body || !body.pool || !body.dex || !body.trade_size_quote) {
      res.status(400).json({ ok: false, error: 'missing required fields' });
      return;
    }

    const tradeSizeSol = Number(body.trade_size_quote);
    if (!(tradeSizeSol > 0)) {
      res.status(400).json({ ok: false, error: 'trade_size_quote must be positive' });
      return;
    }

    const connection = getConnection();
    let buySlippage = 0;
    let sellSlippage = 0;
    let buyFeePct = 0;
    let sellFeePct = 0;

    const fail = (msg: string) => {
      res.status(400).json({ ok: false, error: msg });
    };

    if (body.dex === 'orca_whirlpool') {
      const poolInfo = await fetchOrcaWhirlpoolPool(connection, body.pool);
      if (!poolInfo || !poolInfo.hasWsol || !(poolInfo.price > 0)) {
        return fail('orca pool not supported or missing wSOL');
      }
      const wsolDecimals = poolInfo.wsolIsY ? poolInfo.decimalsY : poolInfo.decimalsX;
      const baseDecimals = poolInfo.wsolIsY ? poolInfo.decimalsX : poolInfo.decimalsY;
      const buyTokenMint = new PublicKey(poolInfo.wsolIsY ? poolInfo.tokenYMint : poolInfo.tokenXMint);
      const sellTokenMint = new PublicKey(poolInfo.wsolIsY ? poolInfo.tokenXMint : poolInfo.tokenYMint);
      const feeRate = poolInfo.feeRate || 0;

      const buyQuote = await quoteOrcaWhirlpoolSwap(
        connection,
        poolInfo.address,
        buyTokenMint,
        tradeSizeSol,
        wsolDecimals,
        baseDecimals,
      );
      const buyExpected = poolInfo.wsolIsY
        ? tradeSizeSol / poolInfo.price
        : tradeSizeSol * poolInfo.price;
      buySlippage = buyExpected > 0 ? ((buyExpected - buyQuote.outAmount) / buyExpected) * 100 : 0;

      const sellBaseAmount = poolInfo.wsolIsY
        ? tradeSizeSol / poolInfo.price
        : tradeSizeSol * poolInfo.price;
      const sellQuote = await quoteOrcaWhirlpoolSwap(
        connection,
        poolInfo.address,
        sellTokenMint,
        sellBaseAmount,
        baseDecimals,
        wsolDecimals,
      );
      const sellExpected = poolInfo.wsolIsY
        ? sellBaseAmount * poolInfo.price
        : sellBaseAmount / poolInfo.price;
      sellSlippage = sellExpected > 0 ? ((sellExpected - sellQuote.outAmount) / sellExpected) * 100 : 0;
      buyFeePct = feeRate * 100;
      sellFeePct = feeRate * 100;
    } else if (body.dex === 'raydium_clmm') {
      const poolInfo = await fetchRaydiumClmmPool(connection, body.pool);
      if (!poolInfo || !poolInfo.clmmPoolInfo || !poolInfo.hasWsol || !(poolInfo.price > 0)) {
        return fail('raydium clmm pool not supported or missing wSOL');
      }
      const wsolDecimals = poolInfo.wsolIsY ? poolInfo.decimalsY : poolInfo.decimalsX;
      const baseDecimals = poolInfo.wsolIsY ? poolInfo.decimalsX : poolInfo.decimalsY;
      const mintA = poolInfo.clmmPoolInfo.mintA.mint as PublicKey;
      const mintB = poolInfo.clmmPoolInfo.mintB.mint as PublicKey;

      const buyBaseMint = poolInfo.wsolIsY ? mintB : mintA;
      const buyQuote = await quoteRaydiumClmmSwap(
        connection,
        poolInfo.clmmPoolInfo,
        tradeSizeSol,
        buyBaseMint,
        wsolDecimals,
        baseDecimals,
      );
      const buyExpected = poolInfo.wsolIsY
        ? tradeSizeSol / poolInfo.price
        : tradeSizeSol * poolInfo.price;
      buySlippage = buyExpected > 0 ? ((buyExpected - buyQuote.outAmount) / buyExpected) * 100 : 0;
      buyFeePct = buyQuote.feePercent;

      const sellBaseAmount = poolInfo.wsolIsY
        ? tradeSizeSol / poolInfo.price
        : tradeSizeSol * poolInfo.price;
      const sellBaseMint = poolInfo.wsolIsY ? mintA : mintB;
      const sellQuote = await quoteRaydiumClmmSwap(
        connection,
        poolInfo.clmmPoolInfo,
        sellBaseAmount,
        sellBaseMint,
        baseDecimals,
        wsolDecimals,
      );
      const sellExpected = poolInfo.wsolIsY
        ? sellBaseAmount * poolInfo.price
        : sellBaseAmount / poolInfo.price;
      sellSlippage = sellExpected > 0 ? ((sellExpected - sellQuote.outAmount) / sellExpected) * 100 : 0;
      sellFeePct = sellQuote.feePercent;
    } else if (body.dex === 'meteora_dlmm') {
      const poolInfo = await fetchMeteoraDlmmPool(connection, body.pool);
      if (!poolInfo || !poolInfo.dlmmPool || !poolInfo.hasWsol || !(poolInfo.price > 0)) {
        return fail('meteora dlmm pool not supported or missing wSOL');
      }
      const wsolDecimals = poolInfo.wsolIsY ? poolInfo.decimalsY : poolInfo.decimalsX;
      const baseDecimals = poolInfo.wsolIsY ? poolInfo.decimalsX : poolInfo.decimalsY;

      const buySwapForY = !poolInfo.wsolIsY;
      const buyQuote = await quoteDlmmSwap(
        poolInfo.dlmmPool,
        tradeSizeSol,
        buySwapForY,
        wsolDecimals,
        baseDecimals,
        poolInfo.price,
      );
      buySlippage = buyQuote.slippagePercent;
      buyFeePct = buyQuote.feePercent;

      const sellBaseAmount = poolInfo.wsolIsY
        ? tradeSizeSol / poolInfo.price
        : tradeSizeSol * poolInfo.price;
      const sellSwapForY = poolInfo.wsolIsY;
      const sellQuote = await quoteDlmmSwap(
        poolInfo.dlmmPool,
        sellBaseAmount,
        sellSwapForY,
        baseDecimals,
        wsolDecimals,
        poolInfo.price,
      );
      sellSlippage = sellQuote.slippagePercent;
      sellFeePct = sellQuote.feePercent;
    } else if (body.dex === 'raydium_amm_v4') {
      const poolInfo = await fetchRaydiumAmmV4Pool(connection, body.pool);
      if (!poolInfo || !poolInfo.hasWsol || !(poolInfo.price > 0)) {
        return fail('raydium amm pool not supported or missing wSOL');
      }
      const reserveQuote = poolInfo.wsolIsY ? poolInfo.reserveYAmount : poolInfo.reserveXAmount;
      const reserveBase = poolInfo.wsolIsY ? poolInfo.reserveXAmount : poolInfo.reserveYAmount;
      const feeRate = poolInfo.feeRate || 0;
      const buyEstimate = estimateSlippageWithFee(reserveBase, reserveQuote, tradeSizeSol, true, feeRate);
      const sellEstimate = estimateSlippageWithFee(reserveBase, reserveQuote, tradeSizeSol, false, feeRate);
      buySlippage = buyEstimate.slippagePercent;
      sellSlippage = sellEstimate.slippagePercent;
      buyFeePct = buyEstimate.feePercent;
      sellFeePct = sellEstimate.feePercent;
    } else if (body.dex === 'raydium_cpmm') {
      const poolInfo = await fetchRaydiumCpmmPool(connection, body.pool);
      if (!poolInfo || !poolInfo.hasWsol || !(poolInfo.price > 0)) {
        return fail('raydium cpmm pool not supported or missing wSOL');
      }
      const reserveQuote = poolInfo.wsolIsY ? poolInfo.reserveYAmount : poolInfo.reserveXAmount;
      const reserveBase = poolInfo.wsolIsY ? poolInfo.reserveXAmount : poolInfo.reserveYAmount;
      const feeRate = poolInfo.feeRate || 0;
      const buyEstimate = estimateSlippageWithFee(reserveBase, reserveQuote, tradeSizeSol, true, feeRate);
      const sellEstimate = estimateSlippageWithFee(reserveBase, reserveQuote, tradeSizeSol, false, feeRate);
      buySlippage = buyEstimate.slippagePercent;
      sellSlippage = sellEstimate.slippagePercent;
      buyFeePct = buyEstimate.feePercent;
      sellFeePct = sellEstimate.feePercent;
    } else if (body.dex === 'meteora_damm_v2') {
      const poolInfo = await fetchMeteoraDammV2Pool(connection, body.pool);
      if (!poolInfo || !poolInfo.hasWsol || !(poolInfo.price > 0)) {
        return fail('meteora damm pool not supported or missing wSOL');
      }
      if (!poolInfo.dammState) {
        return fail('damm state missing');
      }
      const spotPrice = spotQuotePerBase(poolInfo.price, poolInfo.wsolIsY);
      if (!(spotPrice > 0)) {
        return fail('damm spot price invalid');
      }
      const wsolDecimals = poolInfo.wsolIsY ? poolInfo.decimalsY : poolInfo.decimalsX;
      const baseDecimals = poolInfo.wsolIsY ? poolInfo.decimalsX : poolInfo.decimalsY;
      const buyInputIsTokenA = !poolInfo.wsolIsY;
      const sellInputIsTokenA = poolInfo.wsolIsY;

      const buyQuote = dammQuoteExactIn(
        poolInfo.dammState,
        tradeSizeSol,
        buyInputIsTokenA,
        wsolDecimals,
        baseDecimals,
      );
      const baseAmount = tradeSizeSol / spotPrice;
      const sellQuote = dammQuoteExactIn(
        poolInfo.dammState,
        baseAmount,
        sellInputIsTokenA,
        baseDecimals,
        wsolDecimals,
      );

      if (!buyQuote || !sellQuote || !(baseAmount > 0)) {
        return fail('damm quote failed');
      }

      const buyExpected = tradeSizeSol / spotPrice;
      buySlippage = buyExpected > 0
        ? ((buyExpected - buyQuote.outAmount) / buyExpected) * 100
        : 0;
      buyFeePct = buyQuote.feePercent;

      const sellExpected = baseAmount * spotPrice;
      sellSlippage = sellExpected > 0
        ? ((sellExpected - sellQuote.outAmount) / sellExpected) * 100
        : 0;
      sellFeePct = sellQuote.feePercent;
    } else if (body.dex === 'pump_fun_dlmm') {
      const poolInfo = await fetchPumpFunDlmmPool(connection, body.pool);
      if (!poolInfo || !poolInfo.hasWsol || !(poolInfo.price > 0)) {
        return fail('pump.fun dlmm pool not supported or missing wSOL');
      }
      const reserveQuote = poolInfo.wsolIsY ? poolInfo.reserveYAmount : poolInfo.reserveXAmount;
      const reserveBase = poolInfo.wsolIsY ? poolInfo.reserveXAmount : poolInfo.reserveYAmount;
      const feeRate = poolInfo.feeRate || 0;
      const buyEstimate = estimateSlippageWithFee(reserveBase, reserveQuote, tradeSizeSol, true, feeRate);
      const sellEstimate = estimateSlippageWithFee(reserveBase, reserveQuote, tradeSizeSol, false, feeRate);
      buySlippage = buyEstimate.slippagePercent;
      sellSlippage = sellEstimate.slippagePercent;
      buyFeePct = buyEstimate.feePercent;
      sellFeePct = sellEstimate.feePercent;
    } else {
      return fail(`unsupported dex: ${body.dex}`);
    }

    res.json({
      ok: true,
      dex: body.dex,
      pool: body.pool,
      trade_size_quote: tradeSizeSol,
      buy_slippage_pct: buySlippage,
      sell_slippage_pct: sellSlippage,
      buy_fee_pct: buyFeePct,
      sell_fee_pct: sellFeePct,
    });
  } catch (err: any) {
    const message = err && err.message ? String(err.message) : String(err);
    res.status(500).json({ ok: false, error: message });
  }
}
