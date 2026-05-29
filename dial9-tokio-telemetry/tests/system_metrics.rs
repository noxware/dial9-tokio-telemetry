mod common;

#[cfg(unix)]
use common::{BytesCapturingWriter, decode_all};
#[cfg(unix)]
use dial9_tokio_telemetry::telemetry::analysis_events::Dial9Event;
#[cfg(unix)]
use dial9_tokio_telemetry::telemetry::{SystemMetricsConfig, TracedRuntime};

#[cfg(unix)]
#[test]
fn traced_runtime_records_system_metrics() {
    let (writer, batches) = BytesCapturingWriter::new();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(1).enable_all();
    let (runtime, guard) = TracedRuntime::builder()
        .with_system_metrics(SystemMetricsConfig::default())
        .build_and_start_with_writer(builder, writer)
        .unwrap();

    drop(runtime);
    drop(guard);

    let batches = batches.lock().unwrap();
    let events: Vec<Dial9Event> = decode_all(&batches);
    let metrics: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Dial9Event::SystemMetricsEvent(event) => Some(event),
            _ => None,
        })
        .collect();

    assert!(
        !metrics.is_empty(),
        "expected at least one system metrics event"
    );
    assert!(metrics[0].timestamp_ns > 0);
    assert!(metrics[0].max_rss_bytes > 0);
}
