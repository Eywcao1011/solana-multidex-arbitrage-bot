use dashmap::DashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 带 slot 验证和 TTL 的缓存项
#[derive(Clone)]
pub struct CachedValue<T> {
    pub data: T,
    pub slot: u64,
    pub cached_at: Instant,
}

impl<T> CachedValue<T> {
    pub fn new(data: T, slot: u64) -> Self {
        Self {
            data,
            slot,
            cached_at: Instant::now(),
        }
    }

    /// 检查缓存是否仍然有效
    ///
    /// ⚠️ 注意：即使 slot age 满足条件，同一个 slot 内账户也可能被多次更新
    /// 对于流动性账户（tick/bin arrays），这个检查是不够的
    pub fn is_valid(&self, current_slot: u64, max_slot_age: u64, ttl: Duration) -> bool {
        // Slot 不能太旧
        if current_slot > self.slot + max_slot_age {
            return false;
        }

        // 时间不能超过 TTL
        if self.cached_at.elapsed() > ttl {
            return false;
        }

        true
    }
}

/// 带 slot 和 TTL 验证的缓存
///
/// ⚠️ 警告：此缓存无法检测同一 slot 内的多次更新
/// 不适用于频繁变化的账户（如 tick/bin arrays）
pub struct SlotAwareCache<K, V> {
    map: Arc<DashMap<K, CachedValue<V>>>,
    max_slot_age: u64,
    ttl: Duration,
}

impl<K, V> SlotAwareCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new(max_slot_age: u64, ttl: Duration) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            max_slot_age,
            ttl,
        }
    }

    /// 插入缓存值
    pub fn insert(&self, key: K, value: V, slot: u64) {
        self.map.insert(key, CachedValue::new(value, slot));
    }

    /// 获取缓存值，带 slot 验证
    ///
    /// ⚠️ 注意：即使在同一个 slot 内，流动性账户也可能被多次更新
    /// 此方法无法检测这种情况，建议对 tick/bin arrays 禁用缓存
    pub fn get(&self, key: &K, current_slot: u64) -> Option<V> {
        let entry = self.map.get(key)?;
        let cached = entry.value();

        // 检查是否仍然有效
        if cached.is_valid(current_slot, self.max_slot_age, self.ttl) {
            Some(cached.data.clone())
        } else {
            // 过期，删除
            drop(entry);
            self.map.remove(key);
            None
        }
    }

    /// 删除指定 key 的缓存
    pub fn remove(&self, key: &K) -> Option<CachedValue<V>> {
        self.map.remove(key).map(|(_, v)| v)
    }

    /// 清理所有过期的缓存项
    pub fn cleanup_expired(&self, current_slot: u64) {
        self.map
            .retain(|_, value| value.is_valid(current_slot, self.max_slot_age, self.ttl));
    }

    /// 获取缓存大小
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// 清空所有缓存
    pub fn clear(&self) {
        self.map.clear();
    }
}

