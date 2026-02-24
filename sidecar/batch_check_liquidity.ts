#!/usr/bin/env -S npx ts-node
/**
 * 批量池子滑点检查
 * 用法:
 *   cd sidecar && npx ts-node batch_check_liquidity.ts [pools_file] [trade_size_sol] [output_log]
 *
 * 默认:
 *   pools_file     = ../filtered_pools.txt
 *   trade_size_sol = 0.5
 *   output_log     = ../logs/pool_slippage_report.log
 */

import * as fs from 'fs';
import * as path from 'path';
import { spawn } from 'child_process';

const DEFAULT_POOLS_FILE = path.resolve(__dirname, '../filtered_pools.txt');
const DEFAULT_OUTPUT_LOG = path.resolve(__dirname, '../logs/pool_slippage_report.log');
const BASE58_RE = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;

type RunResult = {
    stdout: string;
    stderr: string;
    code: number | null;
};

type Row = {
    pool: string;
    dex: string;
    buyFee: string;
    buySlippage: string;
    sellFee: string;
    sellSlippage: string;
    status: string;
    error: string;
};

function sanitizeField(value: string): string {
    return value.replace(/[\t\r\n]+/g, ' ').trim();
}

function parsePoolsFile(filePath: string): string[] {
    const content = fs.readFileSync(filePath, 'utf8');
    const pools: string[] = [];
    for (const rawLine of content.split('\n')) {
        const line = rawLine.trim();
        if (!line || line.startsWith('#')) continue;
        const matches = line.match(/[1-9A-HJ-NP-Za-km-z]{32,44}/g);
        if (!matches || matches.length === 0) continue;
        const pool = matches[matches.length - 1];
        if (BASE58_RE.test(pool)) {
            pools.push(pool);
        }
    }
    return pools;
}

function runCheck(scriptPath: string, pool: string, tradeSizeSol: number): Promise<RunResult> {
    return new Promise((resolve) => {
        const child = spawn(
            'npx',
            ['ts-node', scriptPath, pool, String(tradeSizeSol)],
            { cwd: __dirname, stdio: ['ignore', 'pipe', 'pipe'] }
        );
        let stdout = '';
        let stderr = '';
        child.stdout.on('data', (chunk) => {
            stdout += chunk.toString();
        });
        child.stderr.on('data', (chunk) => {
            stderr += chunk.toString();
        });
        child.on('close', (code) => resolve({ stdout, stderr, code }));
    });
}

function extractDexType(output: string): string {
    const match = output.match(/✅ DEX 类型:\s*(.+)/);
    return match ? match[1].trim() : 'UNKNOWN';
}

type SideMetrics = {
    fee?: string;
    slippage?: string;
};

function extractMetrics(output: string): { buy: SideMetrics; sell: SideMetrics } {
    const result: { buy: SideMetrics; sell: SideMetrics } = { buy: {}, sell: {} };
    let current: 'buy' | 'sell' | null = null;

    for (const rawLine of output.split('\n')) {
        const line = rawLine.trim();
        if (!line) continue;

        if (line.startsWith('│ 买入') || line.startsWith('买入')) {
            current = 'buy';
            continue;
        }
        if (line.startsWith('│ 卖出') || line.startsWith('卖出')) {
            current = 'sell';
            continue;
        }
        if (!current) continue;

        let match = line.match(/交易费:\s*([-0-9.]+)%/);
        if (match) {
            result[current].fee = match[1];
            continue;
        }
        match = line.match(/总滑点:\s*([-0-9.]+)%/);
        if (match) {
            result[current].slippage = match[1];
            continue;
        }
    }

    return result;
}

function extractError(output: string): string {
    const lines = output
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean);
    for (const line of lines) {
        if (line.startsWith('❌')) return line;
        if (line.includes('Error:')) return line;
        if (line.includes('无法')) return line;
    }
    return '';
}

async function main(): Promise<void> {
    const args = process.argv.slice(2);
    const poolsFile = args[0] ? path.resolve(process.cwd(), args[0]) : DEFAULT_POOLS_FILE;
    const tradeSizeSol = Number(args[1] || '0.5');
    const outputLog = args[2] ? path.resolve(process.cwd(), args[2]) : DEFAULT_OUTPUT_LOG;

    if (!Number.isFinite(tradeSizeSol) || tradeSizeSol <= 0) {
        console.error('trade_size_sol must be a positive number');
        process.exit(1);
    }
    if (!fs.existsSync(poolsFile)) {
        console.error(`pools_file not found: ${poolsFile}`);
        process.exit(1);
    }

    const outputDir = path.dirname(outputLog);
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
    }

    const pools = parsePoolsFile(poolsFile);
    if (pools.length === 0) {
        console.error('no pool addresses found in pools_file');
        process.exit(1);
    }

    const headerLines = [
        `# batch_check_liquidity`,
        `# pools_file=${poolsFile}`,
        `# trade_size_sol=${tradeSizeSol}`,
        `# generated_at=${new Date().toISOString()}`,
        '',
    ];

    const rows: Row[] = [];

    const scriptPath = path.resolve(__dirname, 'check_liquidity.ts');
    for (let i = 0; i < pools.length; i += 1) {
        const pool = pools[i];
        console.log(`[${i + 1}/${pools.length}] checking ${pool}...`);
        const result = await runCheck(scriptPath, pool, tradeSizeSol);
        const combined = `${result.stdout}\n${result.stderr}`;

        const dexType = extractDexType(result.stdout || combined);
        const metrics = extractMetrics(result.stdout || combined);
        const errorLine = extractError(combined);

        const status = metrics.buy.slippage && metrics.sell.slippage && result.code === 0 ? 'OK' : 'ERROR';
        rows.push({
            pool,
            dex: sanitizeField(dexType),
            buyFee: metrics.buy.fee || '',
            buySlippage: metrics.buy.slippage || '',
            sellFee: metrics.sell.fee || '',
            sellSlippage: metrics.sell.slippage || '',
            status,
            error: sanitizeField(errorLine),
        });
    }

    const columns: { key: keyof Row; label: string }[] = [
        { key: 'pool', label: 'POOL' },
        { key: 'dex', label: 'DEX' },
        { key: 'buyFee', label: 'BUY_FEE%' },
        { key: 'buySlippage', label: 'BUY_SLP%' },
        { key: 'sellFee', label: 'SELL_FEE%' },
        { key: 'sellSlippage', label: 'SELL_SLP%' },
        { key: 'status', label: 'STATUS' },
    ];

    const widths = columns.map((col) => {
        const maxValue = rows.reduce((max, row) => {
            const value = row[col.key] || '';
            return Math.max(max, value.length);
        }, col.label.length);
        return maxValue;
    });

    const formatRow = (row: Row, labels: boolean): string => {
        const values = columns.map((col, idx) => {
            const value = labels ? col.label : row[col.key];
            return String(value || '').padEnd(widths[idx], ' ');
        });
        return `| ${values.join(' | ')} |`;
    };

    const divider = `| ${widths.map((w) => '-'.repeat(w)).join(' | ')} |`;

    const outputLines: string[] = [...headerLines];
    outputLines.push(formatRow({} as Row, true));
    outputLines.push(divider);
    for (const row of rows) {
        outputLines.push(formatRow(row, false));
    }

    const errorRows = rows.filter((row) => row.error);
    if (errorRows.length > 0) {
        outputLines.push('');
        outputLines.push('# Errors');
        for (const row of errorRows) {
            outputLines.push(`- ${row.pool} ${row.dex}: ${row.error}`);
        }
    }

    fs.writeFileSync(outputLog, outputLines.join('\n'));
    console.log(`done. report written to ${outputLog}`);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
