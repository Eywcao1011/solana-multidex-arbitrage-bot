use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use chrono::Local;
use once_cell::sync::Lazy;
use std::sync::Mutex;
use tokio::runtime::Handle;

const LOG_DIR: &str = "logs";

static OPP_LOG_PATH: Lazy<Mutex<String>> =
    Lazy::new(|| Mutex::new(compute_log_path("opp")));
static POOL_LOG_PATH: Lazy<Mutex<String>> =
    Lazy::new(|| Mutex::new(compute_log_path("pools")));
static TRADE_LOG_PATH: Lazy<Mutex<String>> =
    Lazy::new(|| Mutex::new(compute_log_path("trade")));

fn compute_log_path(prefix: &str) -> String {
    // 仅使用月日作为后缀，例如 1206
    let date = Local::now().format("%m%d").to_string();

    // 像主 log 一样，按日期 + 递增编号寻找第一个不存在的文件名：
    // prefix_1207.log, prefix_1207_2.log, prefix_1207_3.log, ...
    let mut index = 1;
    loop {
        let name = if index == 1 {
            format!("{}_{}", prefix, date)
        } else {
            format!("{}_{}_{}", prefix, date, index)
        };

        let path = format!("{}/{}.log", LOG_DIR, name);
        if !Path::new(&path).exists() {
            return path;
        }

        index += 1;
    }
}

fn write_line_to_file(path: String, line: String) {
    if let Err(_e) = fs::create_dir_all(Path::new(LOG_DIR)) {
        return;
    }

    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let _ = writeln!(file, "{}", line);
    }
}

fn dispatch_write(path: String, line: String) {
    if let Ok(handle) = Handle::try_current() {
        let _ = handle.spawn_blocking(move || write_line_to_file(path, line));
    } else {
        write_line_to_file(path, line);
    }
}

fn append_line(path_guard: &Mutex<String>, line: &str) {
    let path = {
        let guard = path_guard.lock().unwrap();
        guard.clone()
    };
    dispatch_write(path, line.to_string());
}

pub fn append_opportunity_log(line: &str) {
    let ts = Local::now().to_rfc3339();
    for part in line.split('\n') {
        if part.is_empty() {
            append_line(&OPP_LOG_PATH, "");
        } else {
            let with_ts = format!("[{}] {}", ts, part);
            append_line(&OPP_LOG_PATH, &with_ts);
        }
    }
}

pub fn append_pool_state_log(line: &str) {
    append_line(&POOL_LOG_PATH, line);
}

pub fn append_metrics_log(line: &str) {
    dispatch_write("arb_metrics.txt".to_string(), line.to_string());
}

/// Append a line to the trade execution log (trade lifecycle: BUILD/SIM/SUBMIT/CONFIRM/FAIL)
pub fn append_trade_log(line: &str) {
    append_line(&TRADE_LOG_PATH, line);
}

/// Rotate trade log file (used by dry-run sweeps).
pub fn rotate_trade_log() -> String {
    let path = compute_log_path("trade");
    if let Ok(mut guard) = TRADE_LOG_PATH.lock() {
        *guard = path.clone();
    }
    path
}
