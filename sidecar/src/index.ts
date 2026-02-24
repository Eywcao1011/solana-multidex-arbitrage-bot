import express from 'express';
import cors from 'cors';
import { buildMeteoraDlmmSwapIx, introspectMeteoraDlmmPool, batchIntrospectMeteoraDlmmPools } from './meteora_dlmm';
import { buildOrcaSwapIx, introspectOrcaPool, batchIntrospectOrcaPools } from './orca_whirlpool';
import { buildRaydiumSwapIx, introspectRaydiumPool, batchIntrospectRaydiumPools } from './raydium_clmm';
import { buildRaydiumAmmSwapIx, introspectRaydiumAmmPool, batchIntrospectRaydiumAmmPools } from './raydium_amm';
import { quotePoolSlippage } from './slippage';

const PORT = process.env.SIDECAR_PORT ? parseInt(process.env.SIDECAR_PORT, 10) : 8080;

const app = express();
app.use(cors());
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ ok: true });
});

// ==================== Meteora DLMM ====================
app.post('/meteora/build_swap_ix', async (req, res) => {
  await buildMeteoraDlmmSwapIx(req, res);
});

app.post('/meteora/introspect', async (req, res) => {
  await introspectMeteoraDlmmPool(req, res);
});

app.post('/meteora/batch_introspect', async (req, res) => {
  await batchIntrospectMeteoraDlmmPools(req, res);
});

// ==================== Orca Whirlpool ====================
app.post('/orca/build_swap_ix', async (req, res) => {
  await buildOrcaSwapIx(req, res);
});

app.post('/orca/introspect', async (req, res) => {
  await introspectOrcaPool(req, res);
});

app.post('/orca/batch_introspect', async (req, res) => {
  await batchIntrospectOrcaPools(req, res);
});

// ==================== Raydium CLMM ====================
app.post('/raydium/build_swap_ix', async (req, res) => {
  await buildRaydiumSwapIx(req, res);
});

app.post('/raydium/introspect', async (req, res) => {
  await introspectRaydiumPool(req, res);
});

app.post('/raydium/batch_introspect', async (req, res) => {
  await batchIntrospectRaydiumPools(req, res);
});

// ==================== Raydium AMM V4 ====================
app.post('/raydium_amm/build_swap_ix', async (req, res) => {
  await buildRaydiumAmmSwapIx(req, res);
});

app.post('/raydium_amm/introspect', async (req, res) => {
  await introspectRaydiumAmmPool(req, res);
});

app.post('/raydium_amm/batch_introspect', async (req, res) => {
  await batchIntrospectRaydiumAmmPools(req, res);
});

// ==================== Slippage Quote ====================
app.post('/slippage', async (req, res) => {
  await quotePoolSlippage(req, res);
});

app.listen(PORT, () => {
  console.log(`Sidecar listening on port ${PORT}`);
});
