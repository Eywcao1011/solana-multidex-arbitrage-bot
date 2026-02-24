use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::Path,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use arb::{
    AppContext, DexKind, PoolDescriptor, PriceThresholds, pool_manager::PoolManager,
    pool_manager::PoolMeta,
};
use chrono::{Local, Utc};
use dotenvy::dotenv;
use flexi_logger::{Duplicate, FileSpec, Logger};
use log::{error, info, warn};
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
static ASYNC_HEARTBEAT: AtomicU64 = AtomicU64::new(0);

fn unix_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn init_logging() -> Result<()> {
    let logs_dir = Path::new("logs");
    fs::create_dir_all(logs_dir).context("create logs directory")?;

    let date = Local::now().format("%Y-%m-%d").to_string();
    let mut index = 1;
    let file_base = loop {
        let name = if index == 1 {
            date.clone()
        } else {
            format!("{}_{}", date, index)
        };
        let candidate = logs_dir.join(format!("{name}.log"));
        if !candidate.exists() {
            break name;
        }
        index += 1;
    };

    let file_spec = FileSpec::default()
        .directory(logs_dir)
        .basename(&file_base)
        .suffix("log")
        .suppress_timestamp();

    let _logger = Logger::try_with_env_or_str("debug")
        .context("configure logger")?
        .log_to_file(file_spec)
        .duplicate_to_stdout(Duplicate::Info)
        .format(flexi_logger::detailed_format)
        .append()
        .start()
        .context("start logger")?;

    Ok(())
}

fn spawn_watchdog() {
    tokio::spawn(async move {
        loop {
            ASYNC_HEARTBEAT.store(unix_time_secs(), Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    std::thread::spawn(|| {
        if let Err(err) = fs::create_dir_all("logs") {
            warn!("Failed to create logs directory for watchdog: {}", err);
        }
        loop {
            let ts = Utc::now().to_rfc3339();
            let now = unix_time_secs();
            let async_last = ASYNC_HEARTBEAT.load(Ordering::Relaxed);
            let async_age = now.saturating_sub(async_last);
            let line = format!(
                "{}\tpid={}\tasync_last={}\tasync_age={}s\n",
                ts,
                std::process::id(),
                async_last,
                async_age
            );
            if let Ok(mut file) = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("logs/arb_watchdog.log")
            {
                let _ = file.write_all(line.as_bytes());
            }
            std::thread::sleep(Duration::from_secs(30));
        }
    });
}

fn tokio_worker_threads() -> usize {
    if let Ok(raw) = std::env::var("TOKIO_WORKER_THREADS") {
        if let Ok(parsed) = raw.trim().parse::<usize>() {
            if parsed > 0 {
                return parsed;
            }
        }
        warn!("Invalid TOKIO_WORKER_THREADS={}, using default", raw.trim());
    }
    std::thread::available_parallelism()
        .map(|value| value.get().max(4))
        .unwrap_or(4)
}

fn main() -> Result<()> {
    dotenv().ok();
    init_logging()?;
    let worker_threads = tokio_worker_threads();
    info!("Tokio worker threads: {}", worker_threads);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .context("build tokio runtime")?;

    runtime.block_on(async_main())
}

async fn async_main() -> Result<()> {
    spawn_watchdog();

    let ws_url = std::env::var("SOLANA_WS_URL")
        .unwrap_or_else(|_| "wss://api.mainnet-beta.solana.com".to_string());
    let http_url = std::env::var("SOLANA_HTTP_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());

    let threshold_bps: f64 = std::env::var("ARBITRAGE_ALERT_BPS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(5.0);

    // ✅ Base 币模式配置（原有）
    let trade_size = std::env::var("ARBITRAGE_TRADE_SIZE")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| *value > 0.0);
    let trade_min = std::env::var("ARBITRAGE_TRADE_MIN")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| *value > 0.0);
    let trade_max = std::env::var("ARBITRAGE_TRADE_MAX")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| *value > 0.0);

    // ✅ Quote 币模式配置（新增）
    let trade_size_quote = std::env::var("ARBITRAGE_TRADE_SIZE_QUOTE")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| *value > 0.0);
    let trade_min_quote = std::env::var("ARBITRAGE_TRADE_MIN_QUOTE")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| *value > 0.0);
    let trade_max_quote = std::env::var("ARBITRAGE_TRADE_MAX_QUOTE")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| *value > 0.0);

    // ✅ 验证 Base 模式范围
    let (trade_min, trade_max) = match (trade_min, trade_max) {
        (Some(min), Some(max)) if max > min => (Some(min), Some(max)),
        (Some(min), Some(max)) => {
            warn!(
                "Ignoring ARBITRAGE_TRADE_MIN/MAX (BASE mode) because max ({}) <= min ({}). Fix: set MAX > MIN",
                max, min
            );
            (None, None)
        }
        (Some(_), None) | (None, Some(_)) => {
            warn!("Ignoring partial ARBITRAGE_TRADE_MIN/MAX (BASE mode); both values must be set");
            (None, None)
        }
        (None, None) => (None, None),
    };

    // ✅ 验证 Quote 模式范围
    let (trade_min_quote, trade_max_quote) = match (trade_min_quote, trade_max_quote) {
        (Some(min), Some(max)) if max > min => (Some(min), Some(max)),
        (Some(min), Some(max)) => {
            warn!(
                "Ignoring ARBITRAGE_TRADE_MIN/MAX_QUOTE because max ({}) <= min ({}). Fix: set MAX_QUOTE > MIN_QUOTE",
                max, min
            );
            (None, None)
        }
        (Some(_), None) | (None, Some(_)) => {
            warn!("Ignoring partial ARBITRAGE_TRADE_MIN/MAX_QUOTE; both values must be set");
            (None, None)
        }
        (None, None) => (None, None),
    };

    // ✅ 检测配置冲突：优先使用 Quote 模式
    if trade_size.is_some() && trade_size_quote.is_some() {
        warn!(
            "Both ARBITRAGE_TRADE_SIZE (BASE) and ARBITRAGE_TRADE_SIZE_QUOTE configured; using QUOTE mode"
        );
    }
    if (trade_min.is_some() || trade_max.is_some())
        && (trade_min_quote.is_some() || trade_max_quote.is_some())
    {
        warn!("Both BASE and QUOTE trade range configured; using QUOTE mode");
    }

    let execution_enabled = std::env::var("ENABLE_TRADE_EXECUTION")
        .ok()
        .map(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);

    let thresholds = PriceThresholds {
        alert_bps: threshold_bps,
        trade_size,
        trade_min,
        trade_max,
        trade_size_quote,
        trade_min_quote,
        trade_max_quote,
        execution_enabled,
    };

    info!(
        "Starting monitor with HTTP {} and WS {}; threshold {:.2} bps; execution {}",
        http_url,
        ws_url,
        threshold_bps,
        if execution_enabled {
            "enabled"
        } else {
            "disabled"
        }
    );

    let commitment = CommitmentConfig::confirmed();
    let ctx = AppContext::new(http_url.clone(), commitment);

    info!("Running in spot-only mode (no full subscriptions)");

    // Static pools from env (optional, for backward compatibility or forced subscriptions)
    let static_pools = build_pool_descriptors()?;
    if !static_pools.is_empty() {
        info!(
            "Loaded {} static pool(s) from env (these are always monitored)",
            static_pools.len()
        );
    }

    // ✅ Use PoolManager for dynamic pool discovery from sidecar
    // - PoolManager periodically fetches pools from sidecar endpoints
    // - Applies configured filters (TVL, volume, quote whitelist, etc.)
    // - Automatically subscribes/unsubscribes as pool set changes
    // - Static pools above are always monitored in addition to dynamic pools
    // ✅ Export current pool set to files once at startup (only when EXPORT_POOLS_ONLY=true)
    // When EXPORT_POOLS_ONLY=false, monitor.rs will do the refresh and export
    let export_pools_only = env_bool("EXPORT_POOLS_ONLY", false);
    if export_pools_only {
        info!(
            "EXPORT_POOLS_ONLY=true: fetching pools via PoolManager and writing pool exports to disk"
        );

        let pm = PoolManager::from_env()?;
        let refresh = pm.refresh().await?;

        // Dynamic pools from PoolManager (full metadata)
        let pools_map = pm.get_pools().await;

        let export_pools: Vec<PoolMeta> = pools_map.values().cloned().collect();
        let (final_buf, total_pools, pair_count) = build_pool_export(
            export_pools,
            "Total dynamic pools",
            None,
        );
        fs::write("filtered_pools.txt", final_buf).context("write filtered_pools.txt")?;

        info!(
            "Exported {} dynamic pools across {} arbitrage pairs to filtered_pools.txt (candidates={}, filtered={}, intersection_filtered={}, added={}, removed={})",
            total_pools,
            pair_count,
            refresh.candidates,
            refresh.filtered,
            refresh.intersection_filtered,
            refresh.added,
            refresh.removed
        );



        return Ok(());
    }

    // ⭐ 当 SKIP_WSOL_WRAP=true 时，检查 wSOL ATA 是否存在且有足够余额
    let skip_wsol_wrap = std::env::var("SKIP_WSOL_WRAP")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    
    if skip_wsol_wrap {
        use spl_associated_token_account::get_associated_token_address;
        
        let sol_mint = spl_token::native_mint::id();
        let signer_path = std::env::var("SOLANA_SIGNER_KEYPAIR").ok();
        if let Some(path) = signer_path {
            if let Ok(signer) = solana_keypair::read_keypair_file(&path) {
                let owner = signer.pubkey();
                let wsol_ata = get_associated_token_address(&owner, &sol_mint);
                
                info!("SKIP_WSOL_WRAP=true: checking wSOL ATA {} for owner {}", wsol_ata, owner);
                
                match ctx.rpc_client().get_account(&wsol_ata).await {
                    Ok(account) => {
                        // 解析 wSOL 余额
                        if account.data.len() >= 72 {
                            let amount = u64::from_le_bytes(account.data[64..72].try_into().unwrap_or([0; 8]));
                            let balance_sol = amount as f64 / 1_000_000_000.0;
                            
                            // 获取交易大小配置
                            let trade_size = thresholds.trade_size_quote.unwrap_or(0.7);
                            
                            if balance_sol < trade_size {
                                warn!(
                                    "⚠️ wSOL ATA {} has only {:.6} SOL, but ARBITRAGE_TRADE_SIZE_QUOTE={:.6}. \
                                     Consider funding wSOL ATA or disabling SKIP_WSOL_WRAP",
                                    wsol_ata, balance_sol, trade_size
                                );
                            } else {
                                info!(
                                    "✅ wSOL ATA {} has {:.6} SOL (trade size = {:.6})",
                                    wsol_ata, balance_sol, trade_size
                                );
                            }
                        } else {
                            warn!("⚠️ wSOL ATA {} exists but has invalid data format", wsol_ata);
                        }
                    }
                    Err(e) => {
                        error!(
                            "❌ SKIP_WSOL_WRAP=true but wSOL ATA {} doesn't exist! Error: {}",
                            wsol_ata, e
                        );
                        error!("   Please create and fund wSOL ATA first, or set SKIP_WSOL_WRAP=false");
                        return Err(anyhow::anyhow!(
                            "wSOL ATA {} doesn't exist, required when SKIP_WSOL_WRAP=true",
                            wsol_ata
                        ));
                    }
                }
            } else {
                warn!("SKIP_WSOL_WRAP=true but cannot read signer keypair to check wSOL ATA");
            }
        } else {
            warn!("SKIP_WSOL_WRAP=true but SOLANA_SIGNER_KEYPAIR not set");
        }
    }

    let monitor_enabled = env_bool("ENABLE_MONITOR", true);
    if !monitor_enabled {
        info!("ENABLE_MONITOR=false: monitor disabled, exiting after initialization");
        return Ok(());
    }

    if let Err(err) =
        arb::monitor::run_monitor_with_pool_manager(ctx, ws_url, static_pools, thresholds, commitment).await
    {
        error!("monitor terminated: {err:?}");
    }

    Ok(())
}

/// Read boolean-like env var with default
fn env_bool(var: &str, default: bool) -> bool {
    std::env::var(var)
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}

fn build_pool_export(
    pools: Vec<PoolMeta>,
    total_label: &str,
    header_extra: Option<String>,
) -> (String, usize, usize) {
    let mut pair_groups: HashMap<(Pubkey, Pubkey), Vec<PoolMeta>> = HashMap::new();
    for pool in pools {
        let (min_mint, max_mint) = if pool.base_mint <= pool.quote_mint {
            (pool.base_mint, pool.quote_mint)
        } else {
            (pool.quote_mint, pool.base_mint)
        };
        pair_groups.entry((min_mint, max_mint)).or_default().push(pool);
    }

    let mut grouped: Vec<_> = pair_groups
        .into_iter()
        .filter(|(_, pools)| pools.len() >= 2)
        .collect();

    grouped.sort_by_key(|((a, b), _)| (*a, *b));

    let mut buf = String::new();
    let mut total_pools = 0usize;
    let pair_count = grouped.len();

    let mut dex_counts: HashMap<DexKind, usize> = HashMap::new();
    for ((base, quote), mut pools) in grouped {
        pools.sort_by_key(|p| p.dex as u8);

        let pair_key = format!("{}-{}", base, quote);
        for pool in pools {
            let dex_str = format!("{}", pool.dex);
            let label = pool
                .label
                .clone()
                .unwrap_or_else(|| pool.key.to_string());

            *dex_counts.entry(pool.dex).or_insert(0) += 1;

            buf.push_str(&format!(
                "{}\t{}\t{}\t{}\n",
                pair_key,
                dex_str,
                label,
                pool.key,
            ));
            total_pools += 1;
        }

        buf.push('\n');
    }

    let ts = Local::now().to_rfc3339();
    let mut header = String::new();
    header.push_str(&format!("# Export time: {}\n", ts));
    header.push_str(&format!("# {}: {}\n", total_label, total_pools));
    header.push_str(&format!("# Arbitrage pairs (>=2 pools): {}\n", pair_count));
    if let Some(extra) = header_extra {
        header.push_str(&extra);
        if !extra.ends_with('\n') {
            header.push('\n');
        }
    }
    header.push_str("# Per-DEX counts:\n");

    let mut dex_entries: Vec<_> = dex_counts.into_iter().collect();
    dex_entries.sort_by_key(|(dex, _)| *dex as u8);
    for (dex, count) in dex_entries {
        header.push_str(&format!("#   {:?}: {}\n", dex, count));
    }
    header.push_str("#\n");

    (format!("{}{}", header, buf), total_pools, pair_count)
}



fn build_pool_descriptors() -> Result<Vec<PoolDescriptor>> {
    let mut descriptors = Vec::new();
    load_pool_env("ORCA_POOLS", DexKind::OrcaWhirlpool, &mut descriptors)?;

    // Check if legacy Raydium pools should be disabled
    let disable_raydium_legacy = std::env::var("DISABLE_RAYDIUM_LEGACY")
        .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
        .unwrap_or(false);

    if disable_raydium_legacy {
        log::warn!("DISABLE_RAYDIUM_LEGACY=true: skipping all Raydium CLMM pools");
    } else {
        load_pool_env("RAYDIUM_POOLS", DexKind::RaydiumClmm, &mut descriptors)?;
    }
    load_pool_env(
        "METEORA_DAMM_V1_POOLS",
        DexKind::MeteoraDammV1,
        &mut descriptors,
    )?;
    load_pool_env(
        "METEORA_DAMM_POOLS",
        DexKind::MeteoraDammV2,
        &mut descriptors,
    )?;
    load_pool_env("METEORA_LB_POOLS", DexKind::MeteoraLb, &mut descriptors)?;
    load_pool_env("METEORA_DLMM_POOLS", DexKind::MeteoraDlmm, &mut descriptors)?;
    load_pool_env("PUMP_FUN_POOLS", DexKind::PumpFunDlmm, &mut descriptors)?;

    Ok(descriptors)
}

fn load_pool_env(var_name: &str, kind: DexKind, out: &mut Vec<PoolDescriptor>) -> Result<()> {
    let raw = match std::env::var(var_name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(()),
        Err(err) => return Err(err.into()),
    };

    let mut parsed_entries = Vec::new();
    for entry in raw
        .split(|c| matches!(c, ';' | '\n' | ','))
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        // 允许在条目末尾通过 `|` 附带 Raydium override 信息，这里仅解析出池地址部分
        let pool_part = entry
            .split_once('|')
            .map(|(main, _)| main.trim())
            .unwrap_or(entry);

        let (label, addr) = match pool_part
            .split_once(':')
            .or_else(|| pool_part.split_once('='))
        {
            Some((label, addr)) => {
                let label = label.trim();
                let addr = addr.trim();
                (if label.is_empty() { addr } else { label }, addr)
            }
            None => (pool_part, pool_part),
        };

        parsed_entries.push((label.to_string(), addr.to_string()));
    }

    let mut label_counts: HashMap<String, usize> = HashMap::new();
    for (label, _) in &parsed_entries {
        *label_counts.entry(label.clone()).or_insert(0) += 1;
    }

    let mut label_cursors: HashMap<String, usize> = HashMap::new();
    for (label, addr) in parsed_entries {
        let address = parse_pubkey(&addr)?;
        let count = label_counts.get(&label).copied().unwrap_or(1);
        let final_label = if count > 1 {
            let entry = label_cursors.entry(label.clone()).or_insert(0);
            *entry += 1;
            format!("{} #{}", label, *entry)
        } else {
            label.clone()
        };

        out.push(PoolDescriptor {
            label: final_label,
            address,
            kind,
        });
    }

    Ok(())
}

fn parse_pubkey(value: &str) -> Result<Pubkey> {
    Pubkey::from_str(value).with_context(|| format!("invalid pubkey {value}"))
}

