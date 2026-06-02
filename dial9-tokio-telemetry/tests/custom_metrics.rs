mod common;

use common::BytesCapturingWriter;
use dial9_tokio_telemetry::telemetry::{CustomMetricsConfig, TelemetryCore, TracedRuntime};
use dial9_trace_format::TraceEvent;
use dial9_trace_format::decoder::Decoder;

#[derive(Debug, serde::Deserialize, TraceEvent)]
struct QueuedMetric {
    #[traceevent(timestamp)]
    timestamp_ns: u64,
    value: u64,
}

fn decode_queued_metrics(batches: &[Vec<u8>]) -> Vec<QueuedMetric> {
    let mut metrics = Vec::new();
    for bytes in batches {
        let mut decoder = Decoder::new(bytes).expect("captured batch should be a valid trace");
        decoder
            .for_each_event(|raw| {
                if raw.name == "QueuedMetric" {
                    metrics.push(raw.deserialize().expect("queued metric should decode"));
                }
            })
            .expect("decode batch");
    }
    metrics
}

#[test]
fn traced_runtime_records_custom_metrics_callback_events() {
    let (writer, batches) = BytesCapturingWriter::new();
    let (tx, rx) = std::sync::mpsc::channel();
    tx.send(QueuedMetric {
        timestamp_ns: 1,
        value: 7,
    })
    .unwrap();
    drop(tx);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(1).enable_all();
    let (runtime, guard) = TracedRuntime::builder()
        .with_custom_metrics(CustomMetricsConfig::default(), move |ctx| {
            while let Ok(metric) = rx.try_recv() {
                ctx.record_event(metric);
            }
        })
        .build_and_start_with_writer(builder, writer)
        .unwrap();

    drop(runtime);
    drop(guard);

    let batches = batches.lock().unwrap();
    let metrics = decode_queued_metrics(&batches);

    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].timestamp_ns, 1);
    assert_eq!(metrics[0].value, 7);
}

#[test]
fn telemetry_core_attach_runtime_records_custom_metrics_callback_events() {
    let (writer, batches) = BytesCapturingWriter::new();
    let (tx, rx) = std::sync::mpsc::channel();
    tx.send(QueuedMetric {
        timestamp_ns: 2,
        value: 11,
    })
    .unwrap();
    drop(tx);

    let guard = TelemetryCore::builder().writer(writer).build().unwrap();
    guard.enable();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(1).enable_all();
    let (runtime, _handle) = guard
        .trace_runtime("main")
        .with_custom_metrics(CustomMetricsConfig::default(), move |ctx| {
            while let Ok(metric) = rx.try_recv() {
                ctx.record_event(metric);
            }
        })
        .build(builder)
        .unwrap();

    drop(runtime);
    drop(guard);

    let batches = batches.lock().unwrap();
    let metrics = decode_queued_metrics(&batches);

    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].timestamp_ns, 2);
    assert_eq!(metrics[0].value, 11);
}
