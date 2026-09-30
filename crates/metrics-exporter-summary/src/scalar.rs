use crate::{
    diagnostics::add,
    registry::{Scalar, Series, Shared},
};
use metrics::{CounterFn, GaugeFn};
use std::sync::Arc;

pub(crate) struct ScalarHandle {
    pub shared: Arc<Shared>,
    pub series: Arc<Series>,
}
impl CounterFn for ScalarHandle {
    fn increment(&self, value: u64) {
        if !self.shared.can_record() {
            return;
        }
        let mut scalar = self.series.scalar.lock();
        if !self.shared.running() {
            add(&self.shared.diagnostics.closing_rejections, 1);
            return;
        }
        if let Scalar::Counter { total, pending } = &mut *scalar {
            if let Some(next) = total
                .checked_add(value)
                .filter(|_| value <= i64::MAX as u64 - *pending)
            {
                *total = next;
                // Each stored counter row must fit Int64; the absolute baseline may
                // exceed that range over multiple successfully collected windows.
                *pending += value;
            } else {
                add(&self.shared.diagnostics.arithmetic_overflows, 1);
            }
        }
    }
    fn absolute(&self, value: u64) {
        if !self.shared.can_record() {
            return;
        }
        let mut scalar = self.series.scalar.lock();
        if !self.shared.running() {
            add(&self.shared.diagnostics.closing_rejections, 1);
            return;
        }
        if let Scalar::Counter { total, pending } = &mut *scalar {
            // absolute() still refers to the caller's monotonic counter, not a
            // fresh window. Repeated/older absolute values add no new events.
            if value > *total {
                let increase = value - *total;
                if increase <= i64::MAX as u64 - *pending {
                    *pending += increase;
                    *total = value;
                } else {
                    add(&self.shared.diagnostics.arithmetic_overflows, 1);
                }
            }
        }
    }
}
impl ScalarHandle {
    fn update_gauge(&self, value: f64, operation: impl FnOnce(i64, i64) -> Option<i64>) {
        if !self.shared.can_record() {
            return;
        }
        if !value.is_finite() || value.fract() != 0.0 {
            add(&self.shared.diagnostics.invalid_samples, 1);
            return;
        }
        // The exclusive upper bound matters: i64::MAX rounds to 2^63 as f64.
        if value < i64::MIN as f64 || value >= -(i64::MIN as f64) {
            add(&self.shared.diagnostics.arithmetic_overflows, 1);
            return;
        }
        let value = value as i64;
        let mut scalar = self.series.scalar.lock();
        if !self.shared.running() {
            add(&self.shared.diagnostics.closing_rejections, 1);
            return;
        }
        if let Scalar::Gauge(current) = &mut *scalar {
            if let Some(next) = operation(current.unwrap_or(0), value) {
                *current = Some(next);
            } else {
                add(&self.shared.diagnostics.arithmetic_overflows, 1);
            }
        }
    }
}
impl GaugeFn for ScalarHandle {
    fn increment(&self, value: f64) {
        self.update_gauge(value, i64::checked_add);
    }
    fn decrement(&self, value: f64) {
        self.update_gauge(value, i64::checked_sub);
    }
    fn set(&self, value: f64) {
        self.update_gauge(value, |_, b| Some(b));
    }
}
