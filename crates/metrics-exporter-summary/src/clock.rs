/// Wall-clock source for exported timestamps. Scheduling and I/O deadlines always
/// use `std::time::Instant` and cannot be disrupted by this clock moving backwards.
pub trait Clock: Send + Sync + 'static {
    /// Signed Unix nanoseconds; implementations must return promptly without metrics.
    fn unix_nanos(&self) -> i64;
}

/// The operating system wall clock.
#[derive(Debug, Default)]
pub struct SystemClock;
impl Clock for SystemClock {
    fn unix_nanos(&self) -> i64 {
        crate::unix_nanos()
    }
}
