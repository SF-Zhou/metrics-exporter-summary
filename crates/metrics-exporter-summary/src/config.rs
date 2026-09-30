use std::time::Duration;

/// Bounded recorder resource and scheduling configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// `None` disables automatic sampling; explicit flush/shutdown still collect.
    pub collect_interval: Option<Duration>,
    /// Maximum centroids retained by each digest.
    pub digest_compression: usize,
    /// Raw samples buffered in each thread/series shard before compression.
    pub buffer_capacity: usize,
    /// Maximum logical instrument/label combinations, retained until shutdown.
    /// Defaults to 10,000. The retained-batch byte budget can reject registrations
    /// before this ceiling is reached; it is not a guaranteed usable capacity.
    pub max_series: usize,
    /// Maximum live or not-yet-drained thread/histogram combinations.
    pub max_shards: usize,
    /// Separate budget for describe-only and registered metric names.
    pub max_descriptions: usize,
    /// Total estimated bytes of metric name/description metadata.
    pub max_description_bytes: usize,
    /// Maximum individual description length in UTF-8 bytes.
    pub max_description_length: usize,
    /// Validation and batch-size limits shared with the output model.
    pub validation: metrics_summary_core::ValidationLimits,
    /// Pending plus currently writing batches. In-flight retries retain capacity.
    pub queue_max_batches: usize,
    /// Pending plus currently writing estimated batch bytes.
    pub queue_max_bytes: usize,
    /// Outstanding flush controls, including queued and writer-side waiters.
    pub max_control_requests: usize,
    /// Deadline for one sink attempt.
    pub write_timeout: Duration,
    /// Total time from collection to the end of retries (includes queue wait).
    pub retry_deadline: Duration,
    /// Maximum sink attempts, including the first.
    pub max_write_attempts: usize,
    /// Initial exponential retry delay, with bounded deterministic jitter.
    pub retry_initial_backoff: Duration,
    /// Maximum delay between attempts.
    pub retry_max_backoff: Duration,
    /// Default budget used by `Control::shutdown_default`.
    pub shutdown_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            collect_interval: Some(Duration::from_secs(10)),
            digest_compression: 100,
            buffer_capacity: 32,
            max_series: 10_000,
            max_shards: 200_000,
            max_descriptions: 10_000,
            max_description_bytes: 4 * 1024 * 1024,
            max_description_length: 4096,
            validation: metrics_summary_core::ValidationLimits::default(),
            queue_max_batches: 8,
            queue_max_bytes: 64 * 1024 * 1024,
            max_control_requests: 32,
            write_timeout: Duration::from_secs(5),
            retry_deadline: Duration::from_secs(30),
            max_write_attempts: 5,
            retry_initial_backoff: Duration::from_millis(100),
            retry_max_backoff: Duration::from_secs(2),
            shutdown_timeout: Duration::from_secs(35),
        }
    }
}

/// Invalid or unsupported recorder configuration.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct BuildError(
    /// Human-readable configuration or worker startup failure.
    pub String,
);

impl Config {
    pub(crate) fn validate(&self) -> Result<(), BuildError> {
        self.validation
            .validate()
            .map_err(|e| BuildError(e.to_string()))?;
        let positive = [
            ("digest_compression", self.digest_compression),
            ("buffer_capacity", self.buffer_capacity),
            ("max_series", self.max_series),
            ("max_shards", self.max_shards),
            ("max_descriptions", self.max_descriptions),
            ("max_description_bytes", self.max_description_bytes),
            ("max_description_length", self.max_description_length),
            ("max_batch_bytes", self.validation.max_batch_bytes),
            ("max_rows", self.validation.max_rows),
            ("queue_max_batches", self.queue_max_batches),
            ("queue_max_bytes", self.queue_max_bytes),
            ("max_control_requests", self.max_control_requests),
            ("max_write_attempts", self.max_write_attempts),
        ];
        for (name, value) in positive {
            if value == 0 {
                return Err(BuildError(format!("{name} must be positive")));
            }
        }
        if self.queue_max_bytes < self.validation.max_batch_bytes {
            return Err(BuildError(
                "queue_max_bytes must accommodate max_batch_bytes".into(),
            ));
        }
        if self.max_control_requests > 65_536 {
            return Err(BuildError(
                "max_control_requests must not exceed 65536".into(),
            ));
        }
        if self.collect_interval.is_some_and(|d| d.is_zero()) {
            return Err(BuildError(
                "collect_interval must be positive or None".into(),
            ));
        }
        for (name, duration) in [
            ("write_timeout", self.write_timeout),
            ("retry_deadline", self.retry_deadline),
            ("retry_initial_backoff", self.retry_initial_backoff),
            ("retry_max_backoff", self.retry_max_backoff),
            ("shutdown_timeout", self.shutdown_timeout),
        ] {
            if duration.is_zero() || std::time::Instant::now().checked_add(duration).is_none() {
                return Err(BuildError(format!(
                    "{name} must be positive and representable"
                )));
            }
        }
        if self.retry_initial_backoff > self.retry_max_backoff {
            return Err(BuildError(
                "retry_initial_backoff exceeds retry_max_backoff".into(),
            ));
        }
        if self
            .collect_interval
            .is_some_and(|d| std::time::Instant::now().checked_add(d).is_none())
        {
            return Err(BuildError("collect_interval is too large".into()));
        }
        // Validate arithmetic before any allocation. These are administrator-specified
        // limits, not promises that this much physical memory is available.
        self.max_shards
            .checked_mul(self.buffer_capacity)
            .and_then(|v| v.checked_mul(16))
            .and_then(|v| {
                self.max_shards
                    .checked_mul(self.digest_compression)
                    .and_then(|c| c.checked_mul(128))
                    .and_then(|c| v.checked_add(c))
            })
            .ok_or_else(|| BuildError("shard memory configuration overflows usize".into()))?;
        Ok(())
    }
}
