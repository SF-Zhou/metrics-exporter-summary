use crate::{
    diagnostics::add,
    registry::{Series, Shared},
};
use metrics::HistogramFn;
use parking_lot::Mutex;
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Weak,
    },
};
use tdigest::TDigest;
use uuid::Uuid;

// Shared per OS thread across ALL recorder instances, independent of TLS eviction.
const TLS_CACHE_CAPACITY: usize = 1024;
const MAX_EXACT_DIGEST_COUNT: u64 = 1 << 53;
static NEXT_THREAD: AtomicU64 = AtomicU64::new(1);
thread_local! { static THREAD_STATE: RefCell<ThreadState> = RefCell::new(ThreadState::new()); }

pub(crate) struct Producer {
    pub id: u64,
    pub ended: AtomicBool,
}
struct ThreadState {
    producer: Option<Arc<Producer>>,
    cache: HashMap<(Uuid, u64), Weak<Shard>>,
    order: VecDeque<(Uuid, u64)>,
}
impl ThreadState {
    fn new() -> Self {
        let id = NEXT_THREAD
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .ok();
        Self {
            producer: id.map(|id| {
                Arc::new(Producer {
                    id,
                    ended: AtomicBool::new(false),
                })
            }),
            cache: HashMap::new(),
            order: VecDeque::new(),
        }
    }
    fn insert(&mut self, key: (Uuid, u64), shard: &Arc<Shard>) {
        if let std::collections::hash_map::Entry::Occupied(mut entry) = self.cache.entry(key) {
            entry.insert(Arc::downgrade(shard));
            return;
        }
        if self.cache.len() == TLS_CACHE_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.cache.remove(&oldest);
            }
        }
        self.order.push_back(key);
        self.cache.insert(key, Arc::downgrade(shard));
    }
}
impl Drop for ThreadState {
    fn drop(&mut self) {
        if let Some(producer) = &self.producer {
            producer.ended.store(true, Ordering::Release);
        }
    }
}

pub(crate) struct Shard {
    pub producer: Arc<Producer>,
    pub series: Arc<Series>,
    pub state: Mutex<Accumulator>,
}
pub(crate) struct Accumulator {
    pub epoch: u64,
    pub count: u64,
    pub sum: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub digest: TDigest,
    pending: Vec<f64>,
}
impl Accumulator {
    pub fn new(epoch: u64, compression: usize) -> Self {
        Self {
            epoch,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
            digest: TDigest::new_with_size(compression),
            pending: Vec::new(),
        }
    }
    pub fn compress(&mut self) {
        if !self.pending.is_empty() {
            // Only this external buffer is used; never call the library's buffered
            // push API. Both source statistics and digest stay in the same epoch.
            self.digest = self
                .digest
                .merge_unsorted(std::mem::take(&mut self.pending));
        }
    }
}

pub(crate) struct HistogramHandle {
    pub shared: Arc<Shared>,
    pub series: Arc<Series>,
}
impl HistogramHandle {
    fn shard(&self) -> Option<Arc<Shard>> {
        let key = (self.shared.session, self.series.id);
        let result = THREAD_STATE.try_with(|cell| {
            let Ok(mut tls) = cell.try_borrow_mut() else {
                add(&self.shared.diagnostics.tls_rejections, 1);
                return None;
            };
            if let Some(shard) = tls.cache.get(&key).and_then(Weak::upgrade) {
                return Some(shard);
            }
            let Some(producer) = tls.producer.clone() else {
                add(&self.shared.diagnostics.tls_rejections, 1);
                return None;
            };
            let mut registry = self.shared.registry.lock();
            if !self.shared.running() {
                add(&self.shared.diagnostics.closing_rejections, 1);
                return None;
            }
            let shard_key = (producer.id, self.series.id);
            let shard = if let Some(shard) = registry.shards.get(&shard_key) {
                shard.clone()
            } else {
                if registry.shards.len() >= self.shared.config.max_shards {
                    add(&self.shared.diagnostics.shards_rejected, 1);
                    return None;
                }
                let shard = Arc::new(Shard {
                    producer,
                    series: self.series.clone(),
                    state: Mutex::new(Accumulator::new(
                        registry.epoch,
                        self.shared.config.digest_compression,
                    )),
                });
                registry.shards.insert(shard_key, shard.clone());
                self.shared
                    .diagnostics
                    .active_shards
                    .store(registry.shards.len() as u64, Ordering::Relaxed);
                shard
            };
            drop(registry);
            tls.insert(key, &shard);
            Some(shard)
        });
        match result {
            Ok(shard) => shard,
            Err(_) => {
                add(&self.shared.diagnostics.tls_rejections, 1);
                None
            }
        }
    }
}
impl HistogramFn for HistogramHandle {
    fn record(&self, value: f64) {
        if !self.shared.can_record() {
            return;
        }
        if !value.is_finite() {
            add(&self.shared.diagnostics.invalid_samples, 1);
            return;
        }
        let Some(shard) = self.shard() else {
            return;
        };
        let mut state = shard.state.lock();
        if !self.shared.running() {
            add(&self.shared.diagnostics.closing_rejections, 1);
            return;
        }
        let sum = state.sum + value;
        if !sum.is_finite() || state.count >= MAX_EXACT_DIGEST_COUNT {
            add(&self.shared.diagnostics.arithmetic_overflows, 1);
            return;
        }
        if state.pending.capacity() == 0 {
            state
                .pending
                .reserve_exact(self.shared.config.buffer_capacity);
        }
        state.pending.push(value);
        state.count += 1;
        state.sum = sum;
        state.min = Some(state.min.map_or(value, |m| m.min(value)));
        state.max = Some(state.max.map_or(value, |m| m.max(value)));
        if state.pending.len() >= self.shared.config.buffer_capacity {
            state.compress();
        }
        add(&self.shared.diagnostics.accepted_histogram_samples, 1);
    }
    fn record_many(&self, value: f64, count: usize) {
        // Explicitly O(n), matching metrics' default semantics, with lock release
        // between observations so a large call cannot monopolize a shutdown barrier.
        for _ in 0..count {
            self.record(value);
        }
    }
}
