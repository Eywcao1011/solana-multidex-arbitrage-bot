use dashmap::DashMap;
use std::sync::Arc;

use super::{NormalizedPair, Opportunity, PoolSnapshot};

/// ✅ 优化的套利检测器 - 使用索引避免 O(N²) 复杂度
pub struct OpportunityDetector {
    // 按交易对索引最佳买卖价格
    by_pair: Arc<DashMap<NormalizedPair, PairBestQuotes>>,
}

#[derive(Clone, Debug)]
pub struct PairBestQuotes {
    // ✅ 改为维护top-N池子列表，避免最佳池子价格变差时错失次优池子
    pub best_buys: Vec<PoolSnapshot>, // 按价格从低到高排序（买入方向）
    pub best_sells: Vec<PoolSnapshot>, // 按价格从高到低排序（卖出方向）
}

const MAX_CACHED_POOLS_PER_SIDE: usize = 5;

impl OpportunityDetector {
    pub fn new() -> Self {
        Self {
            by_pair: Arc::new(DashMap::new()),
        }
    }

    /// ✅ O(1) 更新：更新这个池子所属交易对的最佳报价
    /// ⚠️ 修复：维护top-N池子列表，当最佳池子价格变差时可以自动选择次优池子
    pub fn update_pool(&self, snapshot: PoolSnapshot) -> Option<Opportunity> {
        let pair = snapshot.normalized_pair;

        // 获取或创建这个交易对的报价
        let mut entry = self.by_pair.entry(pair).or_insert_with(|| PairBestQuotes {
            best_buys: Vec::new(),
            best_sells: Vec::new(),
        });

        let quotes = entry.value_mut();

        // 更新买入列表（价格从低到高）
        update_pool_list(&mut quotes.best_buys, &snapshot, |a, b| {
            a.normalized_price.partial_cmp(&b.normalized_price).unwrap()
        });

        // 更新卖出列表（价格从高到低）
        update_pool_list(&mut quotes.best_sells, &snapshot, |a, b| {
            b.normalized_price.partial_cmp(&a.normalized_price).unwrap()
        });

        // ✅ 总是重新评估机会，即使最佳报价没有变化
        drop(entry); // 释放锁
        self.check_opportunity_for_pair(&pair)
    }

    /// ✅ O(1) 检测：只检查这个交易对的套利机会
    /// 从top-N列表中选择最佳的买入和卖出池子
    fn check_opportunity_for_pair(&self, pair: &NormalizedPair) -> Option<Opportunity> {
        let entry = self.by_pair.get(pair)?;
        let quotes = entry.value();

        // 遍历 top-N 列表，选择价差最大的有效组合
        let mut best_opportunity: Option<(f64, &PoolSnapshot, &PoolSnapshot)> = None;

        for buy_snapshot in &quotes.best_buys {
            let buy_price = buy_snapshot.normalized_price;
            if buy_price <= 0.0 {
                continue;
            }

            for sell_snapshot in &quotes.best_sells {
                // 必须使用不同的池子
                if buy_snapshot.descriptor.address == sell_snapshot.descriptor.address {
                    continue;
                }

                let sell_price = sell_snapshot.normalized_price;
                if sell_price <= 0.0 {
                    continue;
                }

                let spread = (sell_price - buy_price) / buy_price;
                if spread <= 0.0 {
                    continue;
                }

                match &mut best_opportunity {
                    Some((best_spread, _, _)) if *best_spread >= spread => {}
                    _ => {
                        best_opportunity = Some((spread, buy_snapshot, sell_snapshot));
                    }
                }
            }
        }

        if let Some((spread, buy_snapshot, sell_snapshot)) = best_opportunity {
            Some(Opportunity {
                pair: *pair,
                buy: buy_snapshot.clone(),
                sell: sell_snapshot.clone(),
                spread_pct: spread * 100.0,
                trade: None, // 稍后计算
                tip_lamports: None,
            })
        } else {
            None
        }
    }

    /// 清理过期的池子（可选，定期调用）
    pub fn cleanup_stale(&self, max_age_seconds: i64) {
        use chrono::Utc;

        let now = Utc::now();

        // 收集所有交易对的副本，避免在迭代时修改
        let pairs: Vec<_> = self.by_pair.iter().map(|entry| *entry.key()).collect();

        // 处理每个交易对
        for pair in pairs {
            if let Some(mut entry) = self.by_pair.get_mut(&pair) {
                let quotes = entry.value_mut();

                // 清理过期的买入池子
                quotes
                    .best_buys
                    .retain(|snapshot| (now - snapshot.timestamp).num_seconds() < max_age_seconds);

                // 清理过期的卖出池子
                quotes
                    .best_sells
                    .retain(|snapshot| (now - snapshot.timestamp).num_seconds() < max_age_seconds);

                // 如果两个列表都为空，删除这个交易对
                if quotes.best_buys.is_empty() && quotes.best_sells.is_empty() {
                    drop(entry); // 释放可变引用
                    self.by_pair.remove(&pair);
                }
            }
        }
    }

    /// 获取统计信息
    pub fn stats(&self) -> DetectorStats {
        let mut total_pairs = 0;
        let mut pairs_with_both = 0;

        for entry in self.by_pair.iter() {
            total_pairs += 1;
            if !entry.value().best_buys.is_empty() && !entry.value().best_sells.is_empty() {
                pairs_with_both += 1;
            }
        }

        DetectorStats {
            total_pairs,
            pairs_with_both,
        }
    }
}

pub struct DetectorStats {
    pub total_pairs: usize,
    pub pairs_with_both: usize,
}

/// ✅ 维护top-N池子列表的辅助函数
/// - 如果池子已存在，更新它的快照
/// - 如果池子不存在，插入并保持列表有序
/// - 限制列表大小为 MAX_CACHED_POOLS_PER_SIDE
fn update_pool_list<F>(list: &mut Vec<PoolSnapshot>, new_snapshot: &PoolSnapshot, compare: F)
where
    F: Fn(&PoolSnapshot, &PoolSnapshot) -> std::cmp::Ordering,
{
    // 查找是否已存在同一个池子
    if let Some(pos) = list
        .iter()
        .position(|s| s.descriptor.address == new_snapshot.descriptor.address)
    {
        // 移除旧快照
        list.remove(pos);
    }

    // 插入新快照
    list.push(new_snapshot.clone());

    // 重新排序，确保顺序正确
    list.sort_by(|a, b| compare(a, b));

    // 限制列表大小
    list.truncate(MAX_CACHED_POOLS_PER_SIDE);
}

