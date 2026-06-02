//! User-provided custom metrics callbacks.

use crate::primitives::sync::Arc;
use crate::primitives::sync::atomic::AtomicU64;
use crate::telemetry::buffer::{Encodable, record_encodable_event};
use crate::telemetry::collector::CentralCollector;
use crate::telemetry::events::clock_monotonic_ns;
use crate::telemetry::recorder::source::{FlushContext, Source};
use std::time::{Duration, Instant};

/// Configuration for custom metrics callbacks.
///
/// Built via `CustomMetricsConfig::builder()...build()` and enabled with
/// [`TracedRuntimeBuilder::with_custom_metrics`](crate::telemetry::TracedRuntimeBuilder::with_custom_metrics).
#[derive(Debug, Clone, bon::Builder)]
pub struct CustomMetricsConfig {
    /// Minimum time between callback invocations.
    ///
    /// Defaults to [`Duration::ZERO`], which runs the callback during every
    /// source flush cycle while telemetry is enabled.
    #[builder(default)]
    minimum_interval: Duration,
}

impl Default for CustomMetricsConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl CustomMetricsConfig {
    /// Minimum time between callback invocations.
    ///
    /// [`Duration::ZERO`] means the callback runs during every source flush
    /// cycle while telemetry is enabled.
    pub fn minimum_interval(&self) -> Duration {
        self.minimum_interval
    }
}

/// Context passed to a custom metrics callback.
///
/// Use [`record_event`](Self::record_event) to emit user-defined
/// [`Encodable`] events into the trace.
pub struct CustomMetricsContext<'a> {
    collector: &'a Arc<CentralCollector>,
    drain_epoch: &'a AtomicU64,
    timestamp_ns: u64,
}

impl std::fmt::Debug for CustomMetricsContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomMetricsContext")
            .field("timestamp_ns", &self.timestamp_ns)
            .finish_non_exhaustive()
    }
}

impl CustomMetricsContext<'_> {
    /// Monotonic timestamp captured for this callback invocation.
    pub fn timestamp_ns(&self) -> u64 {
        self.timestamp_ns
    }

    /// Record a user-defined event into the trace.
    pub fn record_event(&mut self, event: impl Encodable) {
        record_encodable_event(&event, self.collector, self.drain_epoch);
    }
}

type CustomMetricsCallback = Box<dyn for<'a> FnMut(&mut CustomMetricsContext<'a>) + Send + 'static>;

pub(crate) struct CustomMetricsSource {
    config: CustomMetricsConfig,
    callback: CustomMetricsCallback,
    last_run: Option<Instant>,
}

impl std::fmt::Debug for CustomMetricsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomMetricsSource")
            .field("config", &self.config)
            .field("last_run", &self.last_run)
            .finish_non_exhaustive()
    }
}

impl CustomMetricsSource {
    pub(crate) fn new<F>(config: CustomMetricsConfig, callback: F) -> Self
    where
        F: for<'a> FnMut(&mut CustomMetricsContext<'a>) + Send + 'static,
    {
        Self {
            config,
            callback: Box::new(callback),
            last_run: None,
        }
    }
}

impl Source for CustomMetricsSource {
    fn flush(&mut self, ctx: &FlushContext<'_>) {
        let now = Instant::now();
        if let Some(last_run) = self.last_run
            && now.duration_since(last_run) < self.config.minimum_interval
        {
            return;
        }
        self.last_run = Some(now);

        let mut custom_ctx = CustomMetricsContext {
            collector: ctx.collector,
            drain_epoch: ctx.drain_epoch,
            timestamp_ns: clock_monotonic_ns(),
        };
        (self.callback)(&mut custom_ctx);
    }

    fn name(&self) -> &'static str {
        "custom_metrics"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::buffer;
    use crate::telemetry::recorder::SharedState;
    use dial9_trace_format::TraceEvent;
    use dial9_trace_format::decoder::Decoder;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, serde::Deserialize, TraceEvent)]
    struct TestMetric {
        #[traceevent(timestamp)]
        timestamp_ns: u64,
        value: u64,
    }

    fn decode_test_metrics(bytes: &[u8]) -> Vec<TestMetric> {
        let mut decoder = Decoder::new(bytes).expect("batch should contain a valid trace");
        let mut events = Vec::new();
        decoder
            .for_each_event(|raw| {
                if raw.name == "TestMetric" {
                    events.push(raw.deserialize().expect("test metric should decode"));
                }
            })
            .expect("decode batch");
        events
    }

    #[test]
    fn default_minimum_interval_runs_every_flush_cycle() {
        assert_eq!(
            CustomMetricsConfig::default().minimum_interval(),
            Duration::ZERO
        );
    }

    #[test]
    fn source_records_callback_events() {
        let shared = SharedState::new(0, None);
        let thread_roles = std::collections::HashMap::new();
        let ctx = FlushContext {
            collector: &shared.collector,
            drain_epoch: &shared.drain_epoch,
            thread_roles: &thread_roles,
        };
        let mut source = CustomMetricsSource::new(CustomMetricsConfig::default(), |ctx| {
            ctx.record_event(TestMetric {
                timestamp_ns: ctx.timestamp_ns(),
                value: 42,
            });
        });

        source.flush(&ctx);
        buffer::drain_to_collector(&shared.collector);

        let batch = shared.collector.next().expect("source should emit a batch");
        let metrics = decode_test_metrics(batch.encoded_bytes());

        assert_eq!(metrics.len(), 1);
        assert!(metrics[0].timestamp_ns > 0);
        assert_eq!(metrics[0].value, 42);
    }

    #[test]
    fn source_respects_minimum_interval() {
        let shared = SharedState::new(0, None);
        let thread_roles = std::collections::HashMap::new();
        let ctx = FlushContext {
            collector: &shared.collector,
            drain_epoch: &shared.drain_epoch,
            thread_roles: &thread_roles,
        };
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let callback_calls = calls.clone();
        let config = CustomMetricsConfig::builder()
            .minimum_interval(Duration::from_secs(60))
            .build();
        let mut source = CustomMetricsSource::new(config, move |_ctx| {
            callback_calls.fetch_add(1, Ordering::Relaxed);
        });

        source.flush(&ctx);
        source.flush(&ctx);

        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn zero_minimum_interval_does_not_throttle() {
        let shared = SharedState::new(0, None);
        let thread_roles = std::collections::HashMap::new();
        let ctx = FlushContext {
            collector: &shared.collector,
            drain_epoch: &shared.drain_epoch,
            thread_roles: &thread_roles,
        };
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let callback_calls = calls.clone();
        let mut source = CustomMetricsSource::new(CustomMetricsConfig::default(), move |_ctx| {
            callback_calls.fetch_add(1, Ordering::Relaxed);
        });

        source.flush(&ctx);
        source.flush(&ctx);

        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }
}
