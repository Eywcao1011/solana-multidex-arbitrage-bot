#!/usr/bin/env -S npx ts-node
/**
 * 池子流动性查询脚本 v2
 * 用于分析套利机会的池子深度和预估滑点
 * 
 * 改进:
 * - Raydium AMM V4: 使用真实可用流动性 (vault - needTakePnl)
 * - 滑点估算: 计入交易费
 * - 明确检测 wSOL 计价池
 * - Orca Whirlpool / Raydium CLMM: SDK quote 估算
 * - DLMM: swapQuote 估算 (含 base + variable fee)
 * - Meteora DAMM V2: 使用 sqrt_price/liquidity 精算
 * - Pump.fun: 粗略估算
 * 
 * 用法:
 *   cd sidecar && npx ts-node check_liquidity.ts <pool_address> [trade_size_sol]
 */

import { Connection, PublicKey } from '@solana/web3.js';
import * as dotenv from 'dotenv';
import * as path from 'path';
import Decimal from 'decimal.js';
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
    type Whirlpool as OrcaWhirlpool,
} from '@orca-so/whirlpools-sdk';
import { Percentage } from '@orca-so/common-sdk';
import { Keypair } from '@solana/web3.js';
import { BondingCurveAccount, GlobalAccount, GLOBAL_ACCOUNT_SEED, DEFAULT_DECIMALS } from 'pumpdotfun-sdk';
import {
    OnlinePumpAmmSdk,
    PUMP_AMM_PROGRAM_ID,
    type Pool as PumpSwapPool,
    type FeeTier as PumpFeeTier,
} from '@pump-fun/pump-swap-sdk';

dotenv.config({ path: path.resolve(__dirname, '../.env') });

const RPC_URL = process.env.RPC_URL || 'https://api.mainnet-beta.solana.com';

const PROGRAM_IDS: { [key: string]: string } = {
    RAYDIUM_AMM_V4: '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
    RAYDIUM_CPMM: 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',
    RAYDIUM_CLMM: 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
    ORCA_WHIRLPOOL: 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
    METEORA_DLMM: 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',
    METEORA_DAMM_V2: 'cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG',
    PUMP_FUN_AMM: '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',
    PUMP_FUN_DLMM: 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',
};

const WSOL_MINT = 'So11111111111111111111111111111111111111112';
const DEFAULT_PUBKEY = new PublicKey('11111111111111111111111111111111');
const SPL_TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const SPL_TOKEN_2022_PROGRAM_ID = 'TokenzQd6A2kYb7dQhYg7F9K4gbpQFZJpR8bthj4GmN';
const PUMP_FUN_GLOBAL_CONFIG_SEED = 'global_config';
const PUMP_FUN_FEE_PROGRAM_ID = 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ';
const PUMP_FUN_FEE_CONFIG_SEED = 'fee_config';
const PUMP_FUN_FEE_CONFIG_STATIC_SEED: number[] = [
    12, 20, 222, 252, 130, 94, 198, 118, 148, 37, 8, 24, 187, 101, 64, 101, 244, 41, 141, 49, 86,
    213, 113, 180, 212, 248, 9, 12, 24, 233, 168, 99,
];

interface PoolInfo {
    address: string;
    dexType: string;
    tokenXMint: string;
    tokenYMint: string;
    reserveXAmount: number;  // 真实可用储备
    reserveYAmount: number;
    rawVaultX?: number;      // 原始 vault 余额
    rawVaultY?: number;
    decimalsX: number;
    decimalsY: number;
    price: number;
    slot: number;
    feeRate?: number;
    feeRaw?: number;
    feeNote?: string;
    activeId?: number;
    binStep?: number;
    hasWsol: boolean;        // 是否包含 wSOL
    wsolIsY: boolean;        // wSOL 是否在 Y 侧
    dlmmPool?: DLMM;
    clmmPoolInfo?: any;
    cpmmPoolInfo?: {
        token0Mint: PublicKey;
        token1Mint: PublicKey;
        token0Vault: PublicKey;
        token1Vault: PublicKey;
        mint0Decimals: number;
        mint1Decimals: number;
        ammConfig: PublicKey;
        observationKey: PublicKey;
    };
    poolProgramId?: string;
    pumpFunCurve?: BondingCurveAccount;
    pumpFunFeeBps?: number;
    dammState?: DammState;
}

interface PumpFunFees {
    lpFeeBps: number;
    protocolFeeBps: number;
    coinCreatorFeeBps: number;
    totalFeeBps: number;
}

interface PumpFunFeeTier {
    thresholdLamports: bigint;
    fees: PumpFunFees;
}

interface PumpFunFeeConfig {
    flatFees: PumpFunFees;
    feeTiers: PumpFunFeeTier[];
    protocolFeeRecipient?: PublicKey;
}

async function getConnection(): Promise<Connection> {
    return new Connection(RPC_URL, 'confirmed');
}

async function getMintMeta(connection: Connection, mint: PublicKey): Promise<{ decimals: number; programId: PublicKey }> {
    const info = await connection.getParsedAccountInfo(mint, 'confirmed');
    if (!info.value) {
        throw new Error('mint account not found');
    }
    const parsed = (info.value.data as any)?.parsed;
    const decimals = typeof parsed?.info?.decimals === 'number' ? parsed.info.decimals : 9;
    const owner = info.value.owner as PublicKey;
    return {
        decimals,
        programId: owner,
    };
}

function percentToNumber(percent: any): number {
    if (!percent || !percent.numerator || !percent.denominator) {
        return 0;
    }
    const numerator = Number(percent.numerator.toString());
    const denominator = Number(percent.denominator.toString());
    if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator === 0) {
        return 0;
    }
    return (numerator / denominator) * 100;
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

function buildEmptyExBitmap(poolId: PublicKey): {
    poolId: PublicKey;
    positiveTickArrayBitmap: BN[][];
    negativeTickArrayBitmap: BN[][];
} {
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

async function identifyDexType(connection: Connection, poolAddress: string): Promise<string | null> {
    try {
        const accountInfo = await connection.getAccountInfo(new PublicKey(poolAddress));
        if (!accountInfo) {
            console.error('Account not found. This address may be invalid or closed.');
            return null;
        }
        const owner = accountInfo.owner.toBase58();
        for (const [name, programId] of Object.entries(PROGRAM_IDS)) {
            if (owner === programId) {
                return name.replace(/_/g, ' ');
            }
        }
        if (owner === SPL_TOKEN_PROGRAM_ID || owner === SPL_TOKEN_2022_PROGRAM_ID) {
            const size = accountInfo.data.length;
            if (size === 82 || size === 165) {
                return `SPL Token Account (size=${size})`;
            }
            return `SPL Token Program (size=${size})`;
        }
        return `Unknown (owner: ${owner})`;
    } catch (e) {
        console.error('RPC error while fetching account info:', e);
        return null;
    }
}

async function fetchRaydiumAmmV4Pool(
    connection: Connection,
    poolAddress: string,
    dexLabel: string = 'Raydium AMM V4'
): Promise<PoolInfo | null> {
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

        // Fee info
        const tradeFeeNumerator = Number(poolState.tradeFeeNumerator.toString());
        const tradeFeeDenominator = Number(poolState.tradeFeeDenominator.toString());
        const feeRate = tradeFeeDenominator > 0 ? tradeFeeNumerator / tradeFeeDenominator : 0;

        // ✅ Fix 1: 获取 needTakePnl (待提取的 PnL)
        const baseNeedTakePnl = Number(poolState.baseNeedTakePnl?.toString() || '0');
        const quoteNeedTakePnl = Number(poolState.quoteNeedTakePnl?.toString() || '0');

        // Fetch vault balances
        const [baseBalance, quoteBalance, slot] = await Promise.all([
            connection.getTokenAccountBalance(baseVault),
            connection.getTokenAccountBalance(quoteVault),
            connection.getSlot(),
        ]);

        const rawVaultX = parseFloat(baseBalance.value.amount);
        const rawVaultY = parseFloat(quoteBalance.value.amount);

        // ✅ 真实可用流动性 = vault 余额 - needTakePnl
        const availableBaseRaw = Math.max(0, rawVaultX - baseNeedTakePnl);
        const availableQuoteRaw = Math.max(0, rawVaultY - quoteNeedTakePnl);

        const reserveXAmount = availableBaseRaw / Math.pow(10, baseDecimal);
        const reserveYAmount = availableQuoteRaw / Math.pow(10, quoteDecimal);
        const price = reserveXAmount > 0 ? reserveYAmount / reserveXAmount : 0;

        // 检测 wSOL
        const hasWsol = baseMint.toBase58() === WSOL_MINT || quoteMint.toBase58() === WSOL_MINT;
        const wsolIsY = quoteMint.toBase58() === WSOL_MINT;

        return {
            address: poolAddress,
            dexType: dexLabel,
            tokenXMint: baseMint.toBase58(),
            tokenYMint: quoteMint.toBase58(),
            reserveXAmount,
            reserveYAmount,
            rawVaultX: rawVaultX / Math.pow(10, baseDecimal),
            rawVaultY: rawVaultY / Math.pow(10, quoteDecimal),
            decimalsX: baseDecimal,
            decimalsY: quoteDecimal,
            price,
            slot,
            feeRate,
            hasWsol,
            wsolIsY,
            poolProgramId: accountInfo.owner.toBase58(),
        };
    } catch (e) {
        console.error('Error fetching Raydium AMM V4 pool:', e);
        return null;
    }
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
        let feeRaw = 25;
        let feeNote = 'default fee (25 bps), amm_config unavailable/invalid';
        if (ammConfigInfo?.data) {
            try {
                const fees = parseRaydiumCpmmConfig(ammConfigInfo.data);
                if (fees.tradeFeeRate > denom) {
                    feeNote = `amm_config trade_fee_rate=${fees.tradeFeeRate} invalid (denom=${denom}), fallback 25 bps`;
                } else {
                    feeRate = fees.tradeFeeRate / denom;
                    feeRaw = fees.tradeFeeRate;
                    feeNote = `amm_config trade_fee_rate=${fees.tradeFeeRate} (denom=${denom}), protocol=${fees.protocolFeeRate}, fund=${fees.fundFeeRate}, creator=${fees.creatorFeeRate}`;
                }
            } catch (e) {
                const errMsg = e instanceof Error ? e.message : String(e);
                feeNote = `amm_config decode failed (${errMsg}), fallback 25 bps`;
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
            feeRaw,
            feeNote,
            hasWsol,
            wsolIsY,
            poolProgramId: accountInfo.owner.toBase58(),
            cpmmPoolInfo: {
                token0Mint: decoded.token0Mint,
                token1Mint: decoded.token1Mint,
                token0Vault: decoded.token0Vault,
                token1Vault: decoded.token1Vault,
                mint0Decimals: decoded.mint0Decimals,
                mint1Decimals: decoded.mint1Decimals,
                ammConfig: decoded.ammConfig,
                observationKey: decoded.observationKey,
            },
        };
    } catch (e) {
        console.error('Error fetching Raydium CPMM pool:', e);
        return null;
    }
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
            console.error('Raydium CLMM: missing amm config account');
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
            decimalsB
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
        } catch (e) {
            console.warn('Raydium CLMM: failed to fetch ex bitmap, using empty bitmap', e);
        }

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
            poolProgramId: programId.toBase58(),
        };
    } catch (e) {
        console.error('Error fetching Raydium CLMM pool:', e);
        return null;
    }
}

async function fetchOrcaWhirlpoolPool(connection: Connection, poolAddress: string): Promise<PoolInfo | null> {
    try {
        const poolPubkey = new PublicKey(poolAddress);

        // Create a dummy wallet for read-only operations
        const dummyKeypair = Keypair.generate();
        const dummyWallet = {
            publicKey: dummyKeypair.publicKey,
            signTransaction: async (tx: any) => tx,
            signAllTransactions: async (txs: any[]) => txs,
        };

        const ctx = WhirlpoolContext.from(connection, dummyWallet as any);
        const client = buildWhirlpoolClient(ctx);
        const whirlpool = await client.getPool(poolPubkey);
        const poolData = whirlpool.getData();

        const tokenAInfo = whirlpool.getTokenAInfo();
        const tokenBInfo = whirlpool.getTokenBInfo();
        const vaultAInfo = whirlpool.getTokenVaultAInfo();
        const vaultBInfo = whirlpool.getTokenVaultBInfo();

        const tokenMintA = tokenAInfo.mint;
        const tokenMintB = tokenBInfo.mint;
        const decimalsA = tokenAInfo.decimals;
        const decimalsB = tokenBInfo.decimals;
        const reserveA = Number(vaultAInfo.amount.toString()) / Math.pow(10, decimalsA);
        const reserveB = Number(vaultBInfo.amount.toString()) / Math.pow(10, decimalsB);

        // Calculate price from sqrtPrice
        const sqrtPriceX64 = poolData.sqrtPrice;
        const sqrtPrice = Number(sqrtPriceX64.toString()) / Math.pow(2, 64);
        const rawPrice = sqrtPrice * sqrtPrice;
        const decimalAdjust = Math.pow(10, decimalsA - decimalsB);
        const price = rawPrice * decimalAdjust;

        const feeRate = poolData.feeRate / 1_000_000;
        const slot = await connection.getSlot();
        const hasWsol = tokenMintA.toBase58() === WSOL_MINT || tokenMintB.toBase58() === WSOL_MINT;
        const wsolIsY = tokenMintB.toBase58() === WSOL_MINT;

        return {
            address: poolAddress,
            dexType: 'Orca Whirlpool',
            tokenXMint: tokenMintA.toBase58(),
            tokenYMint: tokenMintB.toBase58(),
            reserveXAmount: reserveA,
            reserveYAmount: reserveB,
            decimalsX: decimalsA,
            decimalsY: decimalsB,
            price,
            slot,
            feeRate,
            hasWsol,
            wsolIsY,
            poolProgramId: PROGRAM_IDS.ORCA_WHIRLPOOL,
        };
    } catch (e) {
        console.error('Error fetching Orca Whirlpool pool:', e);
        return null;
    }
}

async function fetchMeteoraDlmmPool(
    connection: Connection,
    poolAddress: string,
    dexLabel: string = 'Meteora DLMM',
    programIdOverride?: string
): Promise<PoolInfo | null> {
    try {
        const poolPubkey = new PublicKey(poolAddress);
        const opt = programIdOverride ? { programId: new PublicKey(programIdOverride) } : undefined;
        const dlmmPool = await DLMM.create(connection, poolPubkey, opt);

        const tokenXMint: PublicKey = dlmmPool.lbPair.tokenXMint;
        const tokenYMint: PublicKey = dlmmPool.lbPair.tokenYMint;
        const reserveX: PublicKey = dlmmPool.lbPair.reserveX;
        const reserveY: PublicKey = dlmmPool.lbPair.reserveY;
        const binStep: number = dlmmPool.lbPair.binStep;

        const activeBin = await dlmmPool.getActiveBin();
        const activeId: number = activeBin.binId;

        const [mintXInfo, mintYInfo, balanceX, balanceY, slot] = await Promise.all([
            connection.getParsedAccountInfo(tokenXMint),
            connection.getParsedAccountInfo(tokenYMint),
            connection.getTokenAccountBalance(reserveX),
            connection.getTokenAccountBalance(reserveY),
            connection.getSlot(),
        ]);

        const decimalsX = (mintXInfo.value?.data as any)?.parsed?.info?.decimals ?? 9;
        const decimalsY = (mintYInfo.value?.data as any)?.parsed?.info?.decimals ?? 9;
        const reserveXAmount = parseFloat(balanceX.value.amount) / Math.pow(10, decimalsX);
        const reserveYAmount = parseFloat(balanceY.value.amount) / Math.pow(10, decimalsY);

        let price = 0;
        try {
            const priceStr = dlmmPool.fromPricePerLamport(Number(activeBin.price));
            price = parseFloat(priceStr);
        } catch { }

        // Base fee 计算
        const params = dlmmPool.lbPair.parameters as any;
        const baseFactor = params?.baseFactor ?? 0;
        const feeRate = (baseFactor * binStep) / 10_000_000_000;

        // 检测 wSOL
        const hasWsol = tokenXMint.toBase58() === WSOL_MINT || tokenYMint.toBase58() === WSOL_MINT;
        const wsolIsY = tokenYMint.toBase58() === WSOL_MINT;

        return {
            address: poolAddress,
            dexType: dexLabel,
            tokenXMint: tokenXMint.toBase58(),
            tokenYMint: tokenYMint.toBase58(),
            reserveXAmount,
            reserveYAmount,
            decimalsX,
            decimalsY,
            price,
            slot,
            feeRate,
            activeId,
            binStep,
            hasWsol,
            wsolIsY,
            dlmmPool,
        };
    } catch (e) {
        console.error(`Error fetching ${dexLabel} pool:`, e);
        return null;
    }
}

async function fetchMeteoraDammV2Pool(connection: Connection, poolAddress: string): Promise<PoolInfo | null> {
    try {
        const poolPubkey = new PublicKey(poolAddress);
        const accountInfo = await connection.getAccountInfo(poolPubkey, 'confirmed');
        if (!accountInfo || !accountInfo.data) return null;

        const data = accountInfo.data;
        if (data.length < 360) {
            console.error('Meteora DAMM V2: unexpected account size');
            return null;
        }

        let offset = 8;
        const decodedFees = decodeDammPoolFees(data, offset);
        offset = decodedFees.offset;

        const tokenAMint = readPubkey(data, offset);
        offset += 32;
        const tokenBMint = readPubkey(data, offset);
        offset += 32;
        const tokenAVault = readPubkey(data, offset);
        offset += 32;
        const tokenBVault = readPubkey(data, offset);
        offset += 32;
        offset += 32; // whitelisted vault
        offset += 32; // partner
        const liquidityRaw = readU128LE(data, offset);
        offset += 16;
        offset += 16; // padding
        offset += 8 * 4; // protocol/partner fees
        const sqrtMinPriceX64 = readU128LE(data, offset);
        offset += 16;
        const sqrtMaxPriceX64 = readU128LE(data, offset);
        offset += 16;
        const sqrtPriceX64 = readU128LE(data, offset);
        offset += 16;
        offset += 8; // activation point
        offset += 1; // activation type
        offset += 1; // pool status
        offset += 1; // token a flag
        offset += 1; // token b flag
        offset += 1; // collect fee mode
        offset += 1; // pool type
        offset += 2; // padding

        const [mintAMeta, mintBMeta, balanceA, balanceB, slot] = await Promise.all([
            getMintMeta(connection, tokenAMint),
            getMintMeta(connection, tokenBMint),
            connection.getTokenAccountBalance(tokenAVault),
            connection.getTokenAccountBalance(tokenBVault),
            connection.getSlot(),
        ]);

        const reserveA = parseFloat(balanceA.value.amount) / Math.pow(10, mintAMeta.decimals);
        const reserveB = parseFloat(balanceB.value.amount) / Math.pow(10, mintBMeta.decimals);
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
            maxFeeRatio
        );
        const rawFeeRate = decodedFees.feeInfo.baseFeeRatio + decodedFees.feeInfo.dynamicFeeRatio;
        const feeRate = dammState ? dammState.totalFeeRatio : Math.min(Math.max(rawFeeRate, 0), maxFeeRatio);
        const feeNote = `base=${(decodedFees.feeInfo.baseFeeRatio * 100).toFixed(4)}% dynamic=${(decodedFees.feeInfo.dynamicFeeRatio * 100).toFixed(4)}% protocol=${(decodedFees.feeInfo.protocolFeeRatio * 100).toFixed(4)}% cap=${(maxFeeRatio * 100).toFixed(2)}%`;

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
            feeRaw: Math.round(decodedFees.feeInfo.baseFeeRatio * DAMM_FEE_DENOMINATOR),
            feeNote,
            hasWsol,
            wsolIsY,
            poolProgramId: accountInfo.owner.toBase58(),
            dammState: dammState || undefined,
        };
    } catch (e) {
        console.error('Error fetching Meteora DAMM V2 pool:', e);
        return null;
    }
}

async function fetchPumpFunDlmmFees(connection: Connection): Promise<PumpFunFeeConfig> {
    const sdk = new OnlinePumpAmmSdk(connection);
    const [globalConfig, feeConfig] = await Promise.all([
        sdk.fetchGlobalConfigAccount(),
        sdk.fetchFeeConfigAccount().catch(() => null),
    ]);

    // Convert SDK types to our internal format
    const flatFees = buildPumpFunFees(
        Number(globalConfig.lpFeeBasisPoints.toString()),
        Number(globalConfig.protocolFeeBasisPoints.toString()),
        Number(globalConfig.coinCreatorFeeBasisPoints.toString())
    );

    const protocolFeeRecipient = pickProtocolFeeRecipient(globalConfig.protocolFeeRecipients);
    if (!feeConfig) {
        return { flatFees, feeTiers: [], protocolFeeRecipient };
    }

    // Convert fee tiers from SDK format
    const feeTiers: PumpFunFeeTier[] = feeConfig.feeTiers.map((tier: PumpFeeTier) => ({
        thresholdLamports: BigInt(tier.marketCapLamportsThreshold.toString()),
        fees: buildPumpFunFees(
            Number(tier.fees.lpFeeBps.toString()),
            Number(tier.fees.protocolFeeBps.toString()),
            Number(tier.fees.creatorFeeBps.toString())
        ),
    }));

    const sdkFlatFees = buildPumpFunFees(
        Number(feeConfig.flatFees.lpFeeBps.toString()),
        Number(feeConfig.flatFees.protocolFeeBps.toString()),
        Number(feeConfig.flatFees.creatorFeeBps.toString())
    );

    return { flatFees: sdkFlatFees, feeTiers, protocolFeeRecipient };
}

async function fetchPumpFunDlmmPool(connection: Connection, poolAddress: string): Promise<PoolInfo | null> {
    try {
        const poolPubkey = new PublicKey(poolAddress);
        const sdk = new OnlinePumpAmmSdk(connection);

        // Use SDK to fetch pool data
        const pool: PumpSwapPool = await sdk.fetchPool(poolPubkey);

        // SDK succeeded - use pool data
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

        // Use SDK's lpSupply for market cap calculation
        const lpSupply = BigInt(pool.lpSupply.toString());
        const marketCapLamports = computeMarketCapLamports(lpSupply, baseMeta.decimals, price, quoteMeta.decimals);
        const appliedFees = selectPumpFunFees(feeConfig, marketCapLamports);
        const feeRate = appliedFees.totalFeeBps / 10_000;
        const feeNote = `lp=${appliedFees.lpFeeBps}bps, protocol=${appliedFees.protocolFeeBps}bps, creator=${appliedFees.coinCreatorFeeBps}bps`;

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
            feeRaw: appliedFees.totalFeeBps,
            feeNote,
            hasWsol,
            wsolIsY,
            poolProgramId: PUMP_AMM_PROGRAM_ID.toBase58(),
        };
    } catch (e) {
        console.error('Error fetching Pump.fun DLMM pool:', e);
        return null;
    }
}

async function fetchPumpFunPool(connection: Connection, poolAddress: string): Promise<PoolInfo | null> {
    try {
        const poolPubkey = new PublicKey(poolAddress);
        const accountInfo = await connection.getAccountInfo(poolPubkey, 'confirmed');
        if (!accountInfo || !accountInfo.data) return null;

        let curve: BondingCurveAccount;
        try {
            curve = BondingCurveAccount.fromBuffer(accountInfo.data);
        } catch {
            return null;
        }

        const programId = new PublicKey(PROGRAM_IDS.PUMP_FUN_AMM);
        const [globalAccountPda] = PublicKey.findProgramAddressSync(
            [Buffer.from(GLOBAL_ACCOUNT_SEED)],
            programId
        );
        let feeBps = 0;
        try {
            const globalInfo = await connection.getAccountInfo(globalAccountPda, 'confirmed');
            if (globalInfo?.data) {
                const globalAccount = GlobalAccount.fromBuffer(globalInfo.data);
                feeBps = Number(globalAccount.feeBasisPoints || 0);
            }
        } catch { }

        const reserveToken = Number(curve.virtualTokenReserves) / Math.pow(10, DEFAULT_DECIMALS);
        const reserveSol = Number(curve.virtualSolReserves) / 1_000_000_000;
        const price = reserveToken > 0 ? reserveSol / reserveToken : 0;

        return {
            address: poolAddress,
            dexType: 'Pump.fun AMM',
            tokenXMint: 'unknown',
            tokenYMint: WSOL_MINT,
            reserveXAmount: reserveToken,
            reserveYAmount: reserveSol,
            decimalsX: DEFAULT_DECIMALS,
            decimalsY: 9,
            price,
            slot: await connection.getSlot(),
            feeRate: feeBps / 10_000,
            hasWsol: true,
            wsolIsY: true,
            pumpFunCurve: curve,
            pumpFunFeeBps: feeBps,
            poolProgramId: programId.toBase58(),
        };
    } catch (e) {
        console.error('Error fetching Pump.fun pool:', e);
        return null;
    }
}

function toBaseUnits(amount: number, decimals: number): BN {
    if (!Number.isFinite(amount) || amount <= 0) {
        return new BN(0);
    }
    const fixed = amount.toFixed(decimals);
    const normalized = fixed.replace('.', '');
    return new BN(normalized);
}

function fromBaseUnits(amount: BN, decimals: number): number {
    if (amount.isZero()) return 0;
    return parseFloat(amount.toString()) / Math.pow(10, decimals);
}

function readU64LE(buffer: Buffer, offset: number): bigint {
    return buffer.readBigUInt64LE(offset);
}

function readU128LE(buffer: Buffer, offset: number): bigint {
    const low = buffer.readBigUInt64LE(offset);
    const high = buffer.readBigUInt64LE(offset + 8);
    return (high << 64n) + low;
}

function readU8(buffer: Buffer, offset: number): number {
    return buffer.readUInt8(offset);
}

function readU16LE(buffer: Buffer, offset: number): number {
    return buffer.readUInt16LE(offset);
}

function readU32LE(buffer: Buffer, offset: number): number {
    return buffer.readUInt32LE(offset);
}

function readPubkey(buffer: Buffer, offset: number): PublicKey {
    return new PublicKey(buffer.slice(offset, offset + 32));
}

function parseRaydiumAmmConfig(data: Buffer): {
    tradeFeeRate: number;
    protocolFeeRate: number;
    fundFeeRate: number;
} {
    const minSize = 8 + 1 + 2 + 32 + 4 + 4 + 2 + 4;
    if (data.length < minSize) {
        throw new Error(`Raydium amm_config too short: ${data.length} < ${minSize}`);
    }

    let offset = 8;
    offset += 1; // bump
    offset += 2; // index
    offset += 32; // owner
    const protocolFeeRate = readU32LE(data, offset);
    offset += 4;
    const tradeFeeRate = readU32LE(data, offset);
    offset += 4;
    offset += 2; // tick_spacing
    const fundFeeRate = readU32LE(data, offset);

    return {
        tradeFeeRate,
        protocolFeeRate,
        fundFeeRate,
    };
}

function parseRaydiumCpmmConfig(data: Buffer): {
    tradeFeeRate: number;
    protocolFeeRate: number;
    fundFeeRate: number;
    createPoolFee: number;
    creatorFeeRate: number;
} {
    const minSize = 8 + 1 + 1 + 2 + (4 * 8) + (32 * 2) + 8 + (8 * 15);
    if (data.length < minSize) {
        throw new Error(`CPMM amm_config too short: ${data.length} < ${minSize}`);
    }

    let offset = 8;
    offset += 1; // bump
    offset += 1; // disable_create_pool (bool)
    offset += 2; // index
    const tradeFeeRate = Number(readU64LE(data, offset));
    offset += 8;
    const protocolFeeRate = Number(readU64LE(data, offset));
    offset += 8;
    const fundFeeRate = Number(readU64LE(data, offset));
    offset += 8;
    const createPoolFee = Number(readU64LE(data, offset));
    offset += 8;
    offset += 32; // protocol_owner
    offset += 32; // fund_owner
    const creatorFeeRate = Number(readU64LE(data, offset));

    return {
        tradeFeeRate,
        protocolFeeRate,
        fundFeeRate,
        createPoolFee,
        creatorFeeRate,
    };
}

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

const DAMM_FEE_DENOMINATOR = 1_000_000_000;
const DAMM_Q64 = 18_446_744_073_709_551_616.0;
const DAMM_EPSILON = 1e-12;
const DAMM_MAX_FEE_RATIO = 0.95;
const Q64 = Math.pow(2, 64);

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
    offset += 7; // padding
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
    offset += 16; // padding_1 (u64 * 2)

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

function buildDammState(
    feeInfo: DammFeeInfo,
    liquidityRaw: bigint,
    sqrtPriceX64: bigint,
    sqrtMinPriceX64: bigint,
    sqrtMaxPriceX64: bigint,
    maxFeeRatio: number
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
    outDecimals: number
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

    return {
        outAmount,
        feePercent: feeRatio * 100,
    };
}

function printSlippageAdvice(avgSlippage: number): void {
    const recommendedSlippageBps = Math.ceil((avgSlippage + 0.1) * 100);
    console.log('\n┌─────────────────────────────────────────────────────────────┐');
    console.log('│                         建议                                │');
    console.log('├─────────────────────────────────────────────────────────────┤');
    if (avgSlippage > 1.0) {
        console.log('│ ⚠️  高滑点风险 (>1%): 建议减少交易大小或跳过');
    } else if (avgSlippage > 0.5) {
        console.log('│ ⚠️  中等滑点 (0.5-1%): 需要较高的利差');
    } else {
        console.log('│ ✅ 可接受滑点');
    }
    console.log(`│ 推荐 MAX_SLIPPAGE_BPS: ${recommendedSlippageBps}`);
    console.log('└─────────────────────────────────────────────────────────────┘');
}

function decodeRaydiumCpmmPool(data: Buffer): {
    ammConfig: PublicKey;
    observationKey: PublicKey;
    token0Vault: PublicKey;
    token1Vault: PublicKey;
    token0Mint: PublicKey;
    token1Mint: PublicKey;
    token0Program: PublicKey;
    token1Program: PublicKey;
    lpMint: PublicKey;
    mint0Decimals: number;
    mint1Decimals: number;
} {
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
    const lpMint = readPubkey(data, offset);
    offset += 32;
    const token0Mint = readPubkey(data, offset);
    offset += 32;
    const token1Mint = readPubkey(data, offset);
    offset += 32;
    const token0Program = readPubkey(data, offset);
    offset += 32;
    const token1Program = readPubkey(data, offset);
    offset += 32;
    const observationKey = readPubkey(data, offset);
    offset += 32;

    offset += 1; // auth_bump
    const status = readU8(data, offset);
    offset += 1;
    offset += 1; // lp_mint_decimals
    const mint0Decimals = readU8(data, offset);
    offset += 1;
    const mint1Decimals = readU8(data, offset);

    if ((status & 0b100) !== 0) {
        throw new Error(`CPMM pool swap is disabled (status=${status})`);
    }
    if (token0Mint.equals(DEFAULT_PUBKEY) || token1Mint.equals(DEFAULT_PUBKEY)) {
        throw new Error('CPMM pool has default mint pubkeys (likely wrong layout)');
    }

    return {
        ammConfig,
        observationKey,
        token0Vault,
        token1Vault,
        token0Mint,
        token1Mint,
        token0Program,
        token1Program,
        lpMint,
        mint0Decimals,
        mint1Decimals,
    };
}

function decodePumpFunDlmmPool(data: Buffer): {
    baseMint: PublicKey;
    quoteMint: PublicKey;
    baseVault: PublicKey;
    quoteVault: PublicKey;
    lpSupply: bigint;
    coinCreator: PublicKey;
} {
    const headerSize = 8;
    if (data.length < headerSize + 235) {
        throw new Error('pump.fun pool account too short');
    }
    let offset = headerSize;
    readU8(data, offset); // bump
    offset += 1;
    readU16LE(data, offset); // index
    offset += 2;
    offset += 32; // creator
    const baseMint = readPubkey(data, offset);
    offset += 32;
    const quoteMint = readPubkey(data, offset);
    offset += 32;
    offset += 32; // lp mint
    const baseVault = readPubkey(data, offset);
    offset += 32;
    const quoteVault = readPubkey(data, offset);
    offset += 32;
    const lpSupply = readU64LE(data, offset);
    offset += 8;
    const coinCreator = readPubkey(data, offset);
    return {
        baseMint,
        quoteMint,
        baseVault,
        quoteVault,
        lpSupply,
        coinCreator,
    };
}

function parsePumpFunGlobalConfig(data: Buffer): PumpFunFeeConfig {
    const minSize = 8 + 32 + 8 + 8 + 1 + (32 * 8);
    if (data.length < minSize) {
        throw new Error(`pump.fun global config too short (${data.length})`);
    }
    let offset = 8;
    offset += 32; // admin
    let lpFeeBps = Number(readU64LE(data, offset));
    offset += 8;
    let protocolFeeBps = Number(readU64LE(data, offset));
    offset += 8;
    offset += 1; // disable_flags

    lpFeeBps = sanitizeFeeBps(lpFeeBps);
    protocolFeeBps = sanitizeFeeBps(protocolFeeBps);

    let protocolFeeRecipient: PublicKey | undefined;
    for (let i = 0; i < 8; i += 1) {
        const candidate = readPubkey(data, offset);
        offset += 32;
        if (!protocolFeeRecipient && !candidate.equals(DEFAULT_PUBKEY)) {
            protocolFeeRecipient = candidate;
        }
    }

    let coinCreatorFeeBps = 0;
    if (offset + 8 <= data.length) {
        coinCreatorFeeBps = sanitizeFeeBps(Number(readU64LE(data, offset)));
    }

    return {
        flatFees: buildPumpFunFees(lpFeeBps, protocolFeeBps, coinCreatorFeeBps),
        feeTiers: [],
        protocolFeeRecipient,
    };
}

function decodePumpFunFeeConfig(data: Buffer): PumpFunFeeConfig | null {
    if (!data || data.length < 8 + 1 + 32 + 24) {
        return null;
    }
    let offset = 8;
    offset += 1; // bump
    offset += 32; // admin

    const flatLpFee = Number(readU64LE(data, offset)); offset += 8;
    const flatProtocolFee = Number(readU64LE(data, offset)); offset += 8;
    const flatCreatorFee = Number(readU64LE(data, offset)); offset += 8;
    const flatFees = buildPumpFunFees(flatLpFee, flatProtocolFee, flatCreatorFee);

    if (offset + 4 > data.length) {
        return { flatFees, feeTiers: [] };
    }

    const tierCount = readU32LE(data, offset);
    offset += 4;
    const feeTiers: PumpFunFeeTier[] = [];
    for (let i = 0; i < tierCount; i += 1) {
        if (offset + 16 + 24 > data.length) break;
        const thresholdLamports = readU128LE(data, offset);
        offset += 16;
        const tierLpFee = Number(readU64LE(data, offset)); offset += 8;
        const tierProtocolFee = Number(readU64LE(data, offset)); offset += 8;
        const tierCreatorFee = Number(readU64LE(data, offset)); offset += 8;
        feeTiers.push({
            thresholdLamports,
            fees: buildPumpFunFees(tierLpFee, tierProtocolFee, tierCreatorFee),
        });
    }

    return { flatFees, feeTiers };
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
    quoteDecimals: number
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

// ✅ Fix 2: 滑点估算加入交易费
function estimateSlippageWithFee(
    reserveBase: number,
    reserveQuote: number,
    tradeSizeQuote: number,
    isBuy: boolean,
    feeRate: number  // 交易费率 (如 0.0025 = 0.25%)
): { expectedOut: number; slippagePercent: number; priceImpact: number; feePercent: number } {
    if (reserveBase <= 0 || reserveQuote <= 0) {
        return { expectedOut: 0, slippagePercent: 100, priceImpact: 100, feePercent: 0 };
    }

    const k = reserveBase * reserveQuote;
    const spotPrice = reserveQuote / reserveBase;

    // 扣除 fee 后的有效输入
    const effectiveFee = feeRate;

    if (isBuy) {
        // 买入: SOL -> Base
        const amountIn = tradeSizeQuote;
        const amountInAfterFee = amountIn * (1 - effectiveFee);  // 扣除 fee
        const expectedBaseAtSpot = amountIn / spotPrice;  // 理想情况

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
            feePercent: effectiveFee * 100
        };
    } else {
        // 卖出: Base -> SOL
        const baseIn = tradeSizeQuote / spotPrice;
        const expectedQuote = baseIn * spotPrice;

        const newReserveBase = reserveBase + baseIn;
        const newReserveQuote = k / newReserveBase;
        const grossQuoteOut = reserveQuote - newReserveQuote;
        const actualQuoteOut = grossQuoteOut * (1 - effectiveFee);  // 扣除 fee

        const slippagePercent = ((expectedQuote - actualQuoteOut) / expectedQuote) * 100;
        const effectivePrice = actualQuoteOut / baseIn;
        const priceImpact = ((spotPrice - effectivePrice) / spotPrice) * 100;

        return {
            expectedOut: actualQuoteOut,
            slippagePercent,
            priceImpact,
            feePercent: effectiveFee * 100
        };
    }
}

async function quoteDlmmSwap(
    dlmmPool: DLMM,
    amountIn: number,
    swapForY: boolean,
    inDecimals: number,
    outDecimals: number,
    spotPrice: number
): Promise<{
    outAmount: number;
    minOutAmount: number;
    expectedOut: number;
    slippagePercent: number;
    priceImpactPercent: number;
    feePercent: number;
}> {
    const binArrays = await dlmmPool.getBinArrayForSwap(swapForY);
    const amountInLamports = toBaseUnits(amountIn, inDecimals);
    const allowedSlippageBps = new BN(1);
    const quote = await dlmmPool.swapQuote(amountInLamports, swapForY, allowedSlippageBps, binArrays);

    const outAmount = fromBaseUnits(quote.outAmount, outDecimals);
    const minOutAmount = fromBaseUnits(quote.minOutAmount, outDecimals);
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
        minOutAmount,
        expectedOut,
        slippagePercent,
        priceImpactPercent,
        feePercent,
    };
}

async function quoteRaydiumClmmSwap(
    connection: Connection,
    poolInfo: any,
    amountIn: number,
    baseMint: PublicKey,
    inDecimals: number,
    outDecimals: number
): Promise<{
    outAmount: number;
    minOutAmount: number;
    priceImpactPercent: number;
    feePercent: number;
}> {
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

    return {
        outAmount,
        minOutAmount,
        priceImpactPercent,
        feePercent,
    };
}

async function quoteOrcaWhirlpoolSwap(
    connection: Connection,
    poolAddress: string,
    tokenMintIn: PublicKey,
    amountIn: number,
    inDecimals: number,
    outDecimals: number
): Promise<{ outAmount: number; minOutAmount: number }> {
    // Create a dummy wallet for read-only operations
    const dummyKeypair = Keypair.generate();
    const dummyWallet = {
        publicKey: dummyKeypair.publicKey,
        signTransaction: async (tx: any) => tx,
        signAllTransactions: async (txs: any[]) => txs,
    };

    const ctx = WhirlpoolContext.from(connection, dummyWallet as any);
    const client = buildWhirlpoolClient(ctx);
    const whirlpool = await client.getPool(poolAddress);

    const tokenAmount = new BN(toBaseUnits(amountIn, inDecimals).toString());
    const slippage = Percentage.fromFraction(1, 1000); // 0.1% slippage tolerance

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

async function main() {
    const args = process.argv.slice(2);

    if (args.length === 0) {
        console.log(`
====================================================
     池子流动性查询工具 v2 (Liquidity Checker)
====================================================

用法:
  cd sidecar && npx ts-node check_liquidity.ts <pool_address> [trade_size_sol]

参数:
  pool_address    - 池子地址 (必需，需包含 wSOL)
  trade_size_sol  - 交易大小 (SOL), 默认 0.5

改进:
  ✓ Raydium AMM V4: 使用真实流动性 (vault - needTakePnl)
  ✓ 滑点估算: 计入交易费
  ✓ 明确检测 wSOL 池
  ✓ Orca Whirlpool / Raydium CLMM: SDK quote 估算
  ✓ DLMM: swapQuote 估算 (含 base + variable fee)
  ✓ Meteora DAMM V2: sqrt_price 精算
  ✓ Pump.fun: 粗略估算

示例:
  npx ts-node check_liquidity.ts 8WwcNqdZjCY5Pt7AkhupAFknV2txca9sq6YBkGzLbvdt 0.5
`);
        return;
    }

    const poolAddress = args[0];
    const tradeSizeSol = parseFloat(args[1] || '0.5');

    console.log('\n========================================');
    console.log('     池子流动性分析报告 v2');
    console.log('========================================\n');
    console.log(`池子地址: ${poolAddress}`);
    console.log(`交易大小: ${tradeSizeSol} SOL`);
    console.log(`RPC: ${RPC_URL.substring(0, 50)}...`);
    console.log('');

    const connection = await getConnection();

    console.log('🔍 正在识别池子类型...');
    const dexType = await identifyDexType(connection, poolAddress);

    if (!dexType) {
        console.error('❌ 无法识别池子类型，请检查地址是否正确');
        return;
    }

    console.log(`✅ DEX 类型: ${dexType}\n`);

    console.log('📊 正在获取池子数据...');
    let poolInfo: PoolInfo | null = null;

    if (dexType.includes('RAYDIUM AMM V4')) {
        poolInfo = await fetchRaydiumAmmV4Pool(connection, poolAddress, 'Raydium AMM V4');
    } else if (dexType.includes('RAYDIUM CPMM')) {
        poolInfo = await fetchRaydiumCpmmPool(connection, poolAddress);
    } else if (dexType.includes('RAYDIUM CLMM')) {
        poolInfo = await fetchRaydiumClmmPool(connection, poolAddress);
    } else if (dexType.includes('ORCA WHIRLPOOL')) {
        poolInfo = await fetchOrcaWhirlpoolPool(connection, poolAddress);
    } else if (dexType.includes('METEORA DLMM')) {
        poolInfo = await fetchMeteoraDlmmPool(connection, poolAddress, 'Meteora DLMM');
    } else if (dexType.includes('PUMP FUN DLMM')) {
        poolInfo = await fetchPumpFunDlmmPool(connection, poolAddress);
    } else if (dexType.includes('METEORA DAMM V2')) {
        poolInfo = await fetchMeteoraDammV2Pool(connection, poolAddress);
    } else if (dexType.includes('PUMP FUN AMM')) {
        poolInfo = await fetchPumpFunPool(connection, poolAddress);
    } else {
        console.log(`⚠️  ${dexType} 的详细分析暂不支持`);
        return;
    }

    if (!poolInfo) {
        console.error('❌ 无法获取池子信息');
        return;
    }

    // ✅ Fix 3: 检测并警告非 wSOL 池
    if (!poolInfo.hasWsol) {
        console.log('\n⚠️  警告: 此池不包含 wSOL，以下 SOL 相关分析可能不准确！');
        console.log('         脚本仅适用于 wSOL 计价池\n');
    }

    // 显示池子信息
    console.log('\n┌─────────────────────────────────────────────────────────────┐');
    console.log('│                        池子信息                              │');
    console.log('├─────────────────────────────────────────────────────────────┤');
    console.log(`│ 类型:     ${poolInfo.dexType}`);
    console.log(`│ Token X:  ${poolInfo.tokenXMint.substring(0, 44)}`);
    console.log(`│ Token Y:  ${poolInfo.tokenYMint.substring(0, 44)}`);
    console.log(`│ wSOL 侧:  ${poolInfo.hasWsol ? (poolInfo.wsolIsY ? 'Y (Quote)' : 'X (Base)') : '无 wSOL ⚠️'}`);
    console.log('├─────────────────────────────────────────────────────────────┤');

    if (poolInfo.rawVaultX !== undefined) {
        console.log(`│ Vault X (原始): ${poolInfo.rawVaultX.toFixed(4)}`);
        console.log(`│ Vault Y (原始): ${poolInfo.rawVaultY!.toFixed(4)}`);
    }
    console.log(`│ Reserve X (可用): ${poolInfo.reserveXAmount.toFixed(4)}`);
    console.log(`│ Reserve Y (可用): ${poolInfo.reserveYAmount.toFixed(4)}`);
    console.log(`│ 价格:      ${poolInfo.price.toFixed(8)} Y per X`);
    console.log(`│ Base Fee:  ${((poolInfo.feeRate || 0) * 100).toFixed(4)}%`);
    if (poolInfo.feeNote && poolInfo.feeRaw !== undefined) {
        console.log(`│ Fee Note:  ${poolInfo.feeNote} = ${poolInfo.feeRaw}`);
    }
    console.log(`│ Slot:      ${poolInfo.slot}`);

    if (poolInfo.activeId !== undefined) console.log(`│ Active ID: ${poolInfo.activeId}`);
    if (poolInfo.binStep !== undefined) console.log(`│ Bin Step:  ${poolInfo.binStep}`);
    console.log('└─────────────────────────────────────────────────────────────┘');

    // 计算流动性
    const reserveQuote = poolInfo.wsolIsY ? poolInfo.reserveYAmount : poolInfo.reserveXAmount;
    const reserveBase = poolInfo.wsolIsY ? poolInfo.reserveXAmount : poolInfo.reserveYAmount;

    console.log('\n┌─────────────────────────────────────────────────────────────┐');
    console.log('│                      流动性分析                              │');
    console.log('├─────────────────────────────────────────────────────────────┤');
    console.log(`│ SOL 储备:   ${reserveQuote.toFixed(4)} SOL`);
    console.log(`│ Token 储备: ${reserveBase.toFixed(4)}`);
    console.log(`│ 交易占比:   ${((tradeSizeSol / reserveQuote) * 100).toFixed(4)}% of SOL reserve`);
    console.log('└─────────────────────────────────────────────────────────────┘');

    const isDamm = poolInfo.dexType === 'Meteora DAMM V2';
    const isConstantProduct = [
        'Raydium AMM V4',
        'Raydium CPMM',
        'Pump.fun DLMM',
    ].includes(poolInfo.dexType);

    if (isDamm) {
        console.log('\n┌─────────────────────────────────────────────────────────────┐');
        console.log('│           滑点预估 (DAMM V2 精算)                           │');
        console.log('├─────────────────────────────────────────────────────────────┤');

        if (!poolInfo.hasWsol || poolInfo.price <= 0) {
            console.error('❌ DAMM 精算失败: 非 wSOL 池 / 价格无效');
            console.log('└─────────────────────────────────────────────────────────────┘');
            return;
        }

        const spotPrice = spotQuotePerBase(poolInfo.price, poolInfo.wsolIsY);
        const wsolDecimals = poolInfo.wsolIsY ? poolInfo.decimalsY : poolInfo.decimalsX;
        const baseDecimals = poolInfo.wsolIsY ? poolInfo.decimalsX : poolInfo.decimalsY;
        const buyInputIsTokenA = !poolInfo.wsolIsY;
        const sellInputIsTokenA = poolInfo.wsolIsY;

        if (!poolInfo.dammState || !(spotPrice > 0)) {
            console.error('❌ DAMM 精算失败: 缺少状态或价格无效');
            console.log('└─────────────────────────────────────────────────────────────┘');
            return;
        }

        const buyQuote = dammQuoteExactIn(
            poolInfo.dammState,
            tradeSizeSol,
            buyInputIsTokenA,
            wsolDecimals,
            baseDecimals
        );
        const baseAmount = tradeSizeSol / spotPrice;
        const sellQuote = dammQuoteExactIn(
            poolInfo.dammState,
            baseAmount,
            sellInputIsTokenA,
            baseDecimals,
            wsolDecimals
        );

        if (!buyQuote || !sellQuote || !(baseAmount > 0)) {
            console.error('❌ DAMM 精算失败: quote 结果无效');
            console.log('└─────────────────────────────────────────────────────────────┘');
            return;
        }

        const buyExpected = tradeSizeSol / spotPrice;
        const buySlippage = buyExpected > 0
            ? ((buyExpected - buyQuote.outAmount) / buyExpected) * 100
            : 0;
        const buyEffectivePrice = buyQuote.outAmount > 0 ? tradeSizeSol / buyQuote.outAmount : 0;
        const buyPriceImpact = spotPrice > 0
            ? ((buyEffectivePrice - spotPrice) / spotPrice) * 100
            : 0;

        const sellExpected = baseAmount * spotPrice;
        const sellSlippage = sellExpected > 0
            ? ((sellExpected - sellQuote.outAmount) / sellExpected) * 100
            : 0;
        const sellEffectivePrice = baseAmount > 0 ? sellQuote.outAmount / baseAmount : 0;
        const sellPriceImpact = spotPrice > 0
            ? ((spotPrice - sellEffectivePrice) / spotPrice) * 100
            : 0;

        console.log('│ 买入 (SOL → Token):');
        console.log(`│   预期获得:   ${buyQuote.outAmount.toFixed(6)} token`);
        console.log(`│   价格影响:   ${buyPriceImpact.toFixed(4)}%`);
        console.log(`│   交易费:     ${buyQuote.feePercent.toFixed(4)}%`);
        console.log(`│   总滑点:     ${buySlippage.toFixed(4)}%`);
        console.log('│');
        console.log('│ 卖出 (Token → SOL):');
        console.log(`│   预期获得:   ${sellQuote.outAmount.toFixed(6)} SOL`);
        console.log(`│   价格影响:   ${sellPriceImpact.toFixed(4)}%`);
        console.log(`│   交易费:     ${sellQuote.feePercent.toFixed(4)}%`);
        console.log(`│   总滑点:     ${sellSlippage.toFixed(4)}%`);
        console.log('└─────────────────────────────────────────────────────────────┘');

        const avgSlippage = (buySlippage + sellSlippage) / 2;
        printSlippageAdvice(avgSlippage);
    } else if (isConstantProduct && reserveQuote > 0 && reserveBase > 0) {
        const feeRate = poolInfo.feeRate || 0;

        console.log('\n┌─────────────────────────────────────────────────────────────┐');
        console.log('│              滑点预估 (含交易费)                            │');
        console.log('├─────────────────────────────────────────────────────────────┤');

        const buyEstimate = estimateSlippageWithFee(reserveBase, reserveQuote, tradeSizeSol, true, feeRate);
        const sellEstimate = estimateSlippageWithFee(reserveBase, reserveQuote, tradeSizeSol, false, feeRate);

        console.log('│ 买入 (SOL → Token):');
        console.log(`│   预期获得:   ${buyEstimate.expectedOut.toFixed(6)} token`);
        console.log(`│   价格影响:   ${buyEstimate.priceImpact.toFixed(4)}%`);
        console.log(`│   交易费:     ${buyEstimate.feePercent.toFixed(4)}%`);
        console.log(`│   总滑点:     ${buyEstimate.slippagePercent.toFixed(4)}%`);
        console.log('│');
        console.log('│ 卖出 (Token → SOL):');
        console.log(`│   预期获得:   ${sellEstimate.expectedOut.toFixed(6)} SOL`);
        console.log(`│   价格影响:   ${sellEstimate.priceImpact.toFixed(4)}%`);
        console.log(`│   交易费:     ${sellEstimate.feePercent.toFixed(4)}%`);
        console.log(`│   总滑点:     ${sellEstimate.slippagePercent.toFixed(4)}%`);
        console.log('└─────────────────────────────────────────────────────────────┘');

        const avgSlippage = (buyEstimate.slippagePercent + sellEstimate.slippagePercent) / 2;
        printSlippageAdvice(avgSlippage);
    } else if (poolInfo.dexType === 'Orca Whirlpool') {
        console.log('\n┌─────────────────────────────────────────────────────────────┐');
        console.log('│           滑点预估 (Orca swapQuote)                          │');
        console.log('├─────────────────────────────────────────────────────────────┤');

        if (!poolInfo.hasWsol || poolInfo.price <= 0) {
            console.error('❌ Orca swapQuote 失败: 非 wSOL 池 / 价格无效');
            console.log('└─────────────────────────────────────────────────────────────┘');
            return;
        } else {
            const wsolDecimals = poolInfo.wsolIsY ? poolInfo.decimalsY : poolInfo.decimalsX;
            const baseDecimals = poolInfo.wsolIsY ? poolInfo.decimalsX : poolInfo.decimalsY;
            const buyTokenMint = new PublicKey(poolInfo.wsolIsY ? poolInfo.tokenYMint : poolInfo.tokenXMint);
            const sellTokenMint = new PublicKey(poolInfo.wsolIsY ? poolInfo.tokenXMint : poolInfo.tokenYMint);
            const feeRate = poolInfo.feeRate || 0;
            const reserveQuote = poolInfo.wsolIsY ? poolInfo.reserveYAmount : poolInfo.reserveXAmount;
            const reserveBase = poolInfo.wsolIsY ? poolInfo.reserveXAmount : poolInfo.reserveYAmount;

            try {
                const buyQuote = await quoteOrcaWhirlpoolSwap(
                    connection,
                    poolInfo.address,
                    buyTokenMint,
                    tradeSizeSol,
                    wsolDecimals,
                    baseDecimals
                );
                const buyExpected = poolInfo.wsolIsY
                    ? tradeSizeSol / poolInfo.price
                    : tradeSizeSol * poolInfo.price;
                const buySlippage = buyExpected > 0
                    ? ((buyExpected - buyQuote.outAmount) / buyExpected) * 100
                    : 0;

                const sellBaseAmount = poolInfo.wsolIsY
                    ? tradeSizeSol / poolInfo.price
                    : tradeSizeSol * poolInfo.price;
                const sellQuote = await quoteOrcaWhirlpoolSwap(
                    connection,
                    poolInfo.address,
                    sellTokenMint,
                    sellBaseAmount,
                    baseDecimals,
                    wsolDecimals
                );
                const sellExpected = poolInfo.wsolIsY
                    ? sellBaseAmount * poolInfo.price
                    : sellBaseAmount / poolInfo.price;
                const sellSlippage = sellExpected > 0
                    ? ((sellExpected - sellQuote.outAmount) / sellExpected) * 100
                    : 0;

                console.log('│ 买入 (SOL → Token):');
                console.log(`│   预期获得:   ${buyQuote.outAmount.toFixed(6)} token`);
                console.log(`│   交易费:     ${(feeRate * 100).toFixed(4)}%`);
                console.log(`│   总滑点:     ${buySlippage.toFixed(4)}%`);
                console.log('│');
                console.log('│ 卖出 (Token → SOL):');
                console.log(`│   预期获得:   ${sellQuote.outAmount.toFixed(6)} SOL`);
                console.log(`│   交易费:     ${(feeRate * 100).toFixed(4)}%`);
                console.log(`│   总滑点:     ${sellSlippage.toFixed(4)}%`);
                console.log(`│   说明: 使用约 ${sellBaseAmount.toFixed(6)} token 作为卖出量`);
            } catch (e) {
                const errMsg = e instanceof Error ? e.message : String(e);

                console.error(`❌ Orca swapQuote 失败: ${errMsg}`);
                console.log('└─────────────────────────────────────────────────────────────┘');
                return;
            }
        }
        console.log('└─────────────────────────────────────────────────────────────┘');
    } else if (poolInfo.dexType === 'Raydium CLMM') {
        console.log('\n┌─────────────────────────────────────────────────────────────┐');
        console.log('│          滑点预估 (Raydium CLMM)                             │');
        console.log('├─────────────────────────────────────────────────────────────┤');

        if (!poolInfo.clmmPoolInfo || !poolInfo.hasWsol || poolInfo.price <= 0) {
            console.error('❌ Raydium CLMM quote 失败: 缺少池子信息 / 非 wSOL 池 / 价格无效');
            console.log('└─────────────────────────────────────────────────────────────┘');
            return;
        } else {
            const wsolDecimals = poolInfo.wsolIsY ? poolInfo.decimalsY : poolInfo.decimalsX;
            const baseDecimals = poolInfo.wsolIsY ? poolInfo.decimalsX : poolInfo.decimalsY;
            const mintA = poolInfo.clmmPoolInfo.mintA.mint as PublicKey;
            const mintB = poolInfo.clmmPoolInfo.mintB.mint as PublicKey;

            try {
                const buyBaseMint = poolInfo.wsolIsY ? mintB : mintA;
                const buyQuote = await quoteRaydiumClmmSwap(
                    connection,
                    poolInfo.clmmPoolInfo,
                    tradeSizeSol,
                    buyBaseMint,
                    wsolDecimals,
                    baseDecimals
                );
                const buyExpected = poolInfo.wsolIsY
                    ? tradeSizeSol / poolInfo.price
                    : tradeSizeSol * poolInfo.price;
                const buySlippage = buyExpected > 0
                    ? ((buyExpected - buyQuote.outAmount) / buyExpected) * 100
                    : 0;

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
                    wsolDecimals
                );
                const sellExpected = poolInfo.wsolIsY
                    ? sellBaseAmount * poolInfo.price
                    : sellBaseAmount / poolInfo.price;
                const sellSlippage = sellExpected > 0
                    ? ((sellExpected - sellQuote.outAmount) / sellExpected) * 100
                    : 0;

                console.log('│ 买入 (SOL → Token):');
                console.log(`│   预期获得:   ${buyQuote.outAmount.toFixed(6)} token`);
                console.log(`│   价格影响:   ${buyQuote.priceImpactPercent.toFixed(4)}%`);
                console.log(`│   交易费:     ${buyQuote.feePercent.toFixed(4)}%`);
                console.log(`│   总滑点:     ${buySlippage.toFixed(4)}%`);
                console.log('│');
                console.log('│ 卖出 (Token → SOL):');
                console.log(`│   预期获得:   ${sellQuote.outAmount.toFixed(6)} SOL`);
                console.log(`│   价格影响:   ${sellQuote.priceImpactPercent.toFixed(4)}%`);
                console.log(`│   交易费:     ${sellQuote.feePercent.toFixed(4)}%`);
                console.log(`│   总滑点:     ${sellSlippage.toFixed(4)}%`);
                console.log(`│   说明: 使用约 ${sellBaseAmount.toFixed(6)} token 作为卖出量`);
            } catch (e) {
                const errMsg = e instanceof Error ? e.message : String(e);

                console.error(`❌ Raydium CLMM quote 失败: ${errMsg}`);
                console.log('└─────────────────────────────────────────────────────────────┘');
                return;
            }
        }
        console.log('└─────────────────────────────────────────────────────────────┘');
    } else if (poolInfo.dexType === 'Meteora DLMM') {
        console.log('\n┌─────────────────────────────────────────────────────────────┐');
        console.log('│             滑点预估 (DLMM swapQuote)                        │');
        console.log('├─────────────────────────────────────────────────────────────┤');

        if (!poolInfo.dlmmPool || !poolInfo.hasWsol || poolInfo.price <= 0) {
            console.error('❌ DLMM swapQuote 失败: 缺少 DLMM 实例 / 非 wSOL 池 / 价格无效');
            console.log('└─────────────────────────────────────────────────────────────┘');
            return;
        } else {
            const wsolDecimals = poolInfo.wsolIsY ? poolInfo.decimalsY : poolInfo.decimalsX;
            const baseDecimals = poolInfo.wsolIsY ? poolInfo.decimalsX : poolInfo.decimalsY;

            type DlmmQuote = Awaited<ReturnType<typeof quoteDlmmSwap>>;
            let buyQuote: DlmmQuote | null = null;
            let sellQuote: DlmmQuote | null = null;
            let sellBaseAmount = 0;
            try {
                const buySwapForY = !poolInfo.wsolIsY;
                buyQuote = await quoteDlmmSwap(
                    poolInfo.dlmmPool,
                    tradeSizeSol,
                    buySwapForY,
                    wsolDecimals,
                    baseDecimals,
                    poolInfo.price
                );

                sellBaseAmount = poolInfo.wsolIsY
                    ? tradeSizeSol / poolInfo.price
                    : tradeSizeSol * poolInfo.price;
                const sellSwapForY = poolInfo.wsolIsY;
                sellQuote = await quoteDlmmSwap(
                    poolInfo.dlmmPool,
                    sellBaseAmount,
                    sellSwapForY,
                    baseDecimals,
                    wsolDecimals,
                    poolInfo.price
                );
            } catch (e) {
                const errMsg = e instanceof Error ? e.message : String(e);
                console.error(`❌ DLMM swapQuote 失败: ${errMsg}`);
                console.log('└─────────────────────────────────────────────────────────────┘');
                return;
            }

            if (!buyQuote || !sellQuote) {
                console.error('❌ DLMM swapQuote 失败: 结果为空');
                console.log('└─────────────────────────────────────────────────────────────┘');
                return;
            }

            console.log('│ 买入 (SOL → Token):');
            console.log(`│   预期获得:   ${buyQuote.outAmount.toFixed(6)} token`);
            console.log(`│   价格影响:   ${buyQuote.priceImpactPercent.toFixed(4)}%`);
            console.log(`│   交易费:     ${buyQuote.feePercent.toFixed(4)}%`);
            console.log(`│   总滑点:     ${buyQuote.slippagePercent.toFixed(4)}%`);
            console.log('│');
            console.log('│ 卖出 (Token → SOL):');
            console.log(`│   预期获得:   ${sellQuote.outAmount.toFixed(6)} SOL`);
            console.log(`│   价格影响:   ${sellQuote.priceImpactPercent.toFixed(4)}%`);
            console.log(`│   交易费:     ${sellQuote.feePercent.toFixed(4)}%`);
            console.log(`│   总滑点:     ${sellQuote.slippagePercent.toFixed(4)}%`);
            console.log(`│   说明: 使用约 ${sellBaseAmount.toFixed(6)} token 作为卖出量`);
        }

        console.log('└─────────────────────────────────────────────────────────────┘');

        console.log('\n┌─────────────────────────────────────────────────────────────┐');
        console.log('│                      DLMM 说明                              │');
        console.log('├─────────────────────────────────────────────────────────────┤');
        console.log('│ DLMM 使用离散流动性，滑点取决于 active bin 周围的流动性');
        console.log('│');
        console.log(`│ Base Fee: ${((poolInfo.feeRate || 0) * 100).toFixed(4)}%`);
        console.log('│ swapQuote 已包含 base + variable fee 估算');
        console.log('│');
        console.log('│ 如果出现 ExceededAmountSlippageTolerance (6003) 错误:');
        console.log('│   → 增加 MAX_SLIPPAGE_BPS (建议 80-100)');
        console.log('│   → 减少交易大小');
        console.log('│   → 检查 active_id 是否频繁变化');
        console.log('└─────────────────────────────────────────────────────────────┘');
    } else if (poolInfo.dexType === 'Pump.fun AMM') {
        console.log('\n┌─────────────────────────────────────────────────────────────┐');
        console.log('│             滑点预估 (Pump.fun 曲线)                         │');
        console.log('├─────────────────────────────────────────────────────────────┤');

        if (!poolInfo.pumpFunCurve || poolInfo.price <= 0) {
            console.log('│ ⚠️  无法计算曲线报价 (缺少池子信息 / 价格无效)');
        } else {
            try {
                const curve = poolInfo.pumpFunCurve;
                const feeBps = poolInfo.pumpFunFeeBps || 0;
                const solInLamports = BigInt(Math.round(tradeSizeSol * 1_000_000_000));
                const buyOut = curve.getBuyPrice(solInLamports);
                const buyOutTokens = Number(buyOut) / Math.pow(10, DEFAULT_DECIMALS);
                const buyExpected = tradeSizeSol / poolInfo.price;
                const buySlippage = buyExpected > 0
                    ? ((buyExpected - buyOutTokens) / buyExpected) * 100
                    : 0;

                const sellBaseAmount = tradeSizeSol / poolInfo.price;
                const sellBaseRaw = BigInt(Math.round(sellBaseAmount * Math.pow(10, DEFAULT_DECIMALS)));
                const sellOut = curve.getSellPrice(sellBaseRaw, BigInt(feeBps));
                const sellOutSol = Number(sellOut) / 1_000_000_000;
                const sellExpected = sellBaseAmount * poolInfo.price;
                const sellSlippage = sellExpected > 0
                    ? ((sellExpected - sellOutSol) / sellExpected) * 100
                    : 0;

                console.log('│ 买入 (SOL → Token):');
                console.log(`│   预期获得:   ${buyOutTokens.toFixed(6)} token`);
                console.log(`│   交易费:     ${(feeBps / 100).toFixed(4)}%`);
                console.log(`│   总滑点:     ${buySlippage.toFixed(4)}%`);
                console.log('│');
                console.log('│ 卖出 (Token → SOL):');
                console.log(`│   预期获得:   ${sellOutSol.toFixed(6)} SOL`);
                console.log(`│   交易费:     ${(feeBps / 100).toFixed(4)}%`);
                console.log(`│   总滑点:     ${sellSlippage.toFixed(4)}%`);
                console.log(`│   说明: 使用约 ${sellBaseAmount.toFixed(6)} token 作为卖出量`);
            } catch (e) {
                console.log('│ ⚠️  曲线报价失败 (可能已完成曲线或余额不足)');
            }
        }
        console.log('└─────────────────────────────────────────────────────────────┘');
    }

    console.log('\n========================================');
    console.log('             分析完成');
    console.log('========================================\n');
}

main().catch(console.error);
