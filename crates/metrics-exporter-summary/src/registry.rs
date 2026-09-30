use crate::{
    config::BuildError,
    control::Control,
    diagnostics::{add, Diagnostics},
    histogram::{HistogramHandle, Shard},
    scalar::ScalarHandle,
    Config,
};
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_summary_core::{
    effective_labels, Batch, BatchId, MetricKind, MetricValue, Row, Sink, Source, MODEL_VERSION,
};
use parking_lot::{Mutex, RwLock};
use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Instant,
};
use uuid::Uuid;

pub(crate) const RUNNING: u8 = 0;
pub(crate) const CLOSING: u8 = 1;
pub(crate) const CLOSED: u8 = 2;

thread_local! { static SUPPRESSED: Cell<bool> = const { Cell::new(false) }; }

/// Suppress the recorder on its own worker threads so instrumented storage/logging
/// clients cannot create a self-sustaining metrics feedback loop.
pub(crate) fn suppress() {
    let _ = SUPPRESSED.try_with(|flag| flag.set(true));
}
pub(crate) fn is_suppressed() -> bool {
    SUPPRESSED.try_with(Cell::get).unwrap_or(true)
}

/// Configures a recorder and starts its dedicated sampler and writer threads.
pub struct Builder {
    source: Source,
    config: Config,
    clock: Arc<dyn crate::Clock>,
}
impl Builder {
    /// Uses explicit source metadata, including a required hostname.
    pub fn new(source: Source) -> Self {
        Self {
            source,
            config: Config::default(),
            clock: Arc::new(crate::SystemClock),
        }
    }
    /// Discovers the OS hostname. Failure to discover it is a build error.
    pub fn for_service(
        application: impl Into<String>,
        instance: impl Into<String>,
    ) -> Result<Self, BuildError> {
        Source::new(application, instance)
            .map(Self::new)
            .map_err(|e| BuildError(e.to_string()))
    }
    /// Replaces the complete recorder configuration.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }
    /// Overrides exported wall timestamps, for deterministic boundary/clock-step tests.
    pub fn clock(mut self, clock: Arc<dyn crate::Clock>) -> Self {
        self.clock = clock;
        self
    }
    /// Starts a locally usable recorder; does not install global process state.
    pub fn build<S: Sink>(self, sink: S) -> Result<(SummaryRecorder, Control), BuildError> {
        self.config.validate()?;
        self.source
            .validate(&self.config.validation)
            .map_err(|e| BuildError(e.to_string()))?;
        effective_labels(&self.source, &BTreeMap::new(), &self.config.validation)
            .map_err(|e| BuildError(e.to_string()))?;
        let session = Uuid::new_v4();
        let started_at = Instant::now();
        let empty = Batch {
            model_version: MODEL_VERSION,
            id: BatchId {
                source_session_id: session,
                sequence: 1,
            },
            source: self.source.clone(),
            timestamp: self.clock.unix_nanos(),
            duration_ns: 0,
            rows: vec![],
        };
        empty
            .validate(&self.config.validation)
            .map_err(|e| BuildError(e.to_string()))?;
        let shared = Arc::new(Shared {
            source: self.source,
            session,
            started_at,
            config: self.config,
            clock: self.clock,
            registry: Mutex::new(Registry {
                reserved_batch_bytes: empty.estimated_bytes(),
                ..Registry::default()
            }),
            lifecycle: AtomicU8::new(RUNNING),
            diagnostics: Diagnostics::default(),
            last_error: Mutex::new(None),
        });
        let control = Control::start(shared.clone(), Box::new(sink))?;
        Ok((SummaryRecorder { shared }, control))
    }
}

/// A `metrics::Recorder` with shared logical series and OS-thread histogram shards.
/// Clone is cheap and refers to the same recorder instance.
#[derive(Clone)]
pub struct SummaryRecorder {
    pub(crate) shared: Arc<Shared>,
}

impl SummaryRecorder {
    /// Installs this recorder once for the process. Keep the returned build control
    /// handle elsewhere to explicitly flush and shut down.
    pub fn install(self) -> Result<(), metrics::SetRecorderError<Self>> {
        metrics::set_global_recorder(self)
    }
    /// Unique identifier for this recorder session.
    pub fn source_session_id(&self) -> Uuid {
        self.shared.session
    }
}

pub(crate) struct Shared {
    pub source: Source,
    pub session: Uuid,
    pub started_at: Instant,
    pub config: Config,
    pub clock: Arc<dyn crate::Clock>,
    pub registry: Mutex<Registry>,
    pub lifecycle: AtomicU8,
    pub diagnostics: Diagnostics,
    pub last_error: Mutex<Option<crate::LastWriteError>>,
}
impl Shared {
    pub fn note_error(&self, error: &metrics_summary_core::WriteError) {
        *self.last_error.lock() = Some(crate::LastWriteError {
            timestamp_unix_ns: self.clock.unix_nanos(),
            error: metrics_summary_core::WriteError::new(
                error.kind,
                error.outcome,
                error.message.chars().take(1024).collect::<String>(),
            ),
        });
    }
    pub fn running(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == RUNNING
    }
    pub fn can_record(&self) -> bool {
        if is_suppressed() {
            return false;
        }
        if !self.running() {
            add(&self.diagnostics.closing_rejections, 1);
            return false;
        }
        true
    }
    pub fn close_admission(&self) {
        let _guard = self.registry.lock();
        let _ =
            self.lifecycle
                .compare_exchange(RUNNING, CLOSING, Ordering::AcqRel, Ordering::Acquire);
    }
    pub fn finish(&self) {
        let mut registry = self.registry.lock();
        *registry = Registry::default();
        self.diagnostics.active_shards.store(0, Ordering::Relaxed);
        self.diagnostics
            .registered_series
            .store(0, Ordering::Relaxed);
        self.lifecycle.store(CLOSED, Ordering::Release);
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub(crate) struct Identity {
    kind: MetricKind,
    name: String,
    labels: BTreeMap<String, String>,
}

#[derive(Default)]
pub(crate) struct Registry {
    pub series: HashMap<Identity, Arc<Series>>,
    pub shards: HashMap<(u64, u64), Arc<Shard>>,
    descriptions: HashMap<(MetricKind, String), Arc<RwLock<Description>>>,
    description_bytes: usize,
    reserved_batch_bytes: usize,
    next_metric_id: u64,
    pub epoch: u64,
}
pub(crate) struct Description {
    unit: Option<String>,
    text: String,
}
pub(crate) struct Series {
    pub id: u64,
    name: String,
    labels: BTreeMap<String, String>,
    description: Arc<RwLock<Description>>,
    pub scalar: Mutex<Scalar>,
}
pub(crate) enum Scalar {
    // Retain the logical total for CounterFn::absolute; only pending is exported.
    Counter { total: u64, pending: u64 },
    Gauge(Option<i64>),
    Histogram,
}
impl Series {
    pub fn row(&self, value: MetricValue) -> Row {
        Row {
            metric_id: self.id,
            name: self.name.clone(),
            labels: self.labels.clone(),
            unit: self.description.read().unit.clone(),
            value,
        }
    }
}

impl SummaryRecorder {
    fn describe(&self, kind: MetricKind, name: KeyName, unit: Option<Unit>, text: SharedString) {
        if !self.shared.can_record() {
            return;
        }
        let limits = &self.shared.config.validation;
        let name = name.as_str();
        let text = text.as_ref();
        let unit = unit.map(|u| u.as_str().to_owned());
        if name.is_empty()
            || name.len() > limits.max_name_bytes
            || name.chars().any(char::is_control)
            || text.len() > self.shared.config.max_description_length
            || unit
                .as_ref()
                .is_some_and(|u| u.len() > limits.max_unit_bytes)
        {
            add(&self.shared.diagnostics.descriptions_rejected, 1);
            return;
        }
        let mut registry = self.shared.registry.lock();
        if !self.shared.running() {
            add(&self.shared.diagnostics.closing_rejections, 1);
            return;
        }
        let key = (kind, name.to_owned());
        if let Some(existing) = registry.descriptions.get(&key).cloned() {
            let mut desc = existing.write();
            if desc.unit.is_some() && unit.is_some() && desc.unit != unit {
                add(&self.shared.diagnostics.unit_conflicts, 1);
            }
            // Retain the first nonempty description and first valid unit. This is
            // deterministic and does not let repeated describe calls grow storage.
            let extra = if desc.text.is_empty() { text.len() } else { 0 }
                + if desc.unit.is_none() {
                    unit.as_ref().map_or(0, String::len)
                } else {
                    0
                };
            if registry.description_bytes.saturating_add(extra)
                > self.shared.config.max_description_bytes
            {
                add(&self.shared.diagnostics.descriptions_rejected, 1);
                return;
            }
            if desc.text.is_empty() {
                desc.text = text.to_owned();
            }
            if desc.unit.is_none() {
                desc.unit = unit;
            }
            registry.description_bytes += extra;
        } else {
            let bytes = metadata_bytes(name, text, unit.as_deref());
            if registry.descriptions.len() >= self.shared.config.max_descriptions
                || registry.description_bytes.saturating_add(bytes)
                    > self.shared.config.max_description_bytes
            {
                add(&self.shared.diagnostics.descriptions_rejected, 1);
                return;
            }
            registry.descriptions.insert(
                key,
                Arc::new(RwLock::new(Description {
                    unit,
                    text: text.to_owned(),
                })),
            );
            registry.description_bytes += bytes;
        }
    }

    fn register(&self, kind: MetricKind, key: &Key) -> Option<Arc<Series>> {
        if !self.shared.can_record() {
            return None;
        }
        let limits = &self.shared.config.validation;
        // Check borrowed input before allocating canonical owned strings.
        if key.name().is_empty()
            || key.name().len() > limits.max_name_bytes
            || key.name().chars().any(char::is_control)
            || key.labels().count() > limits.max_labels
        {
            add(&self.shared.diagnostics.registrations_rejected, 1);
            return None;
        }
        let mut labels = BTreeMap::new();
        for label in key.labels() {
            if label.key().is_empty()
                || label.key().len() > limits.max_label_key_bytes
                || label.value().len() > limits.max_label_value_bytes
                || label.key().chars().any(char::is_control)
                || labels
                    .insert(label.key().to_owned(), label.value().to_owned())
                    .is_some()
            {
                add(&self.shared.diagnostics.registrations_rejected, 1);
                return None;
            }
        }
        let labels = match effective_labels(&self.shared.source, &labels, limits) {
            Ok(labels) => labels,
            Err(_) => {
                add(&self.shared.diagnostics.registrations_rejected, 1);
                return None;
            }
        };
        let identity = Identity {
            kind,
            name: key.name().to_owned(),
            labels,
        };
        let mut registry = self.shared.registry.lock();
        if !self.shared.running() {
            add(&self.shared.diagnostics.closing_rejections, 1);
            return None;
        }
        if let Some(series) = registry.series.get(&identity) {
            return Some(series.clone());
        }
        // Both scalar kinds use the counters table without a type discriminator.
        // A shared name/label identity cannot mean both a delta and a gauge.
        let other_scalar_kind = match kind {
            MetricKind::Counter => Some(MetricKind::Gauge),
            MetricKind::Gauge => Some(MetricKind::Counter),
            MetricKind::Histogram => None,
        };
        if other_scalar_kind.is_some_and(|kind| {
            registry.series.contains_key(&Identity {
                kind,
                name: identity.name.clone(),
                labels: identity.labels.clone(),
            })
        }) {
            add(&self.shared.diagnostics.registrations_rejected, 1);
            return None;
        }
        if registry.series.len() >= self.shared.config.max_series.min(limits.max_rows)
            || registry.next_metric_id == u64::MAX
        {
            add(&self.shared.diagnostics.registrations_rejected, 1);
            return None;
        }
        // Reserve every possible row plus its future unit, even for currently idle
        // histograms. A valid registration can never make all future batches oversized.
        let sample = Row {
            metric_id: 1,
            name: identity.name.clone(),
            labels: identity.labels.clone(),
            unit: None,
            value: MetricValue::CounterDelta { delta_value: 0 },
        };
        if sample.validate(limits).is_err() {
            add(&self.shared.diagnostics.registrations_rejected, 1);
            return None;
        }
        let reservation = sample
            .estimated_bytes()
            .saturating_add(limits.max_unit_bytes)
            .saturating_add(32);
        if registry.reserved_batch_bytes.saturating_add(reservation) > limits.max_batch_bytes {
            add(&self.shared.diagnostics.registrations_rejected, 1);
            return None;
        }
        let desc_key = (kind, identity.name.clone());
        let description = if let Some(description) = registry.descriptions.get(&desc_key) {
            description.clone()
        } else {
            let bytes = metadata_bytes(&identity.name, "", None);
            if registry.descriptions.len() >= self.shared.config.max_descriptions
                || registry.description_bytes.saturating_add(bytes)
                    > self.shared.config.max_description_bytes
            {
                add(&self.shared.diagnostics.registrations_rejected, 1);
                return None;
            }
            let description = Arc::new(RwLock::new(Description {
                unit: None,
                text: String::new(),
            }));
            registry.descriptions.insert(desc_key, description.clone());
            registry.description_bytes += bytes;
            description
        };
        registry.next_metric_id += 1;
        let series = Arc::new(Series {
            id: registry.next_metric_id,
            name: identity.name.clone(),
            labels: identity.labels.clone(),
            description,
            scalar: Mutex::new(match kind {
                MetricKind::Counter => Scalar::Counter {
                    total: 0,
                    pending: 0,
                },
                MetricKind::Gauge => Scalar::Gauge(None),
                MetricKind::Histogram => Scalar::Histogram,
            }),
        });
        registry.reserved_batch_bytes += reservation;
        registry.series.insert(identity, series.clone());
        self.shared
            .diagnostics
            .registered_series
            .store(registry.series.len() as u64, Ordering::Relaxed);
        Some(series)
    }
}

fn metadata_bytes(name: &str, description: &str, unit: Option<&str>) -> usize {
    256usize
        .saturating_add(name.len())
        .saturating_add(description.len())
        .saturating_add(unit.map_or(0, str::len))
}

impl Recorder for SummaryRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(MetricKind::Counter, key, unit, description);
    }
    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(MetricKind::Gauge, key, unit, description);
    }
    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(MetricKind::Histogram, key, unit, description);
    }
    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        self.register(MetricKind::Counter, key)
            .map(|series| {
                Counter::from_arc(Arc::new(ScalarHandle {
                    shared: self.shared.clone(),
                    series,
                }))
            })
            .unwrap_or_else(Counter::noop)
    }
    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        self.register(MetricKind::Gauge, key)
            .map(|series| {
                Gauge::from_arc(Arc::new(ScalarHandle {
                    shared: self.shared.clone(),
                    series,
                }))
            })
            .unwrap_or_else(Gauge::noop)
    }
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        self.register(MetricKind::Histogram, key)
            .map(|series| {
                Histogram::from_arc(Arc::new(HistogramHandle {
                    shared: self.shared.clone(),
                    series,
                }))
            })
            .unwrap_or_else(Histogram::noop)
    }
}
