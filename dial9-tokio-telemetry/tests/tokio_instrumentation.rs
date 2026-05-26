use dial9_tokio_telemetry::analysis_unstable::TraceReader;
use dial9_tokio_telemetry::telemetry::{RotatingWriter, TelemetryEvent, TracedRuntime};
use std::time::Duration;

#[test]
fn tokio_instrumentation_can_be_disabled_without_disabling_explicit_custom_events() {
    use dial9_trace_format::TraceEvent as TraceEventDerive;

    #[derive(TraceEventDerive)]
    struct ProfilerOnlyMarker {
        #[traceevent(timestamp)]
        timestamp_ns: u64,
        request_count: u32,
    }

    let dir = tempfile::tempdir().unwrap();
    let trace_path = dir.path().join("trace.bin");
    let writer = RotatingWriter::single_file(&trace_path).unwrap();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(2).enable_all();

    let (runtime, guard) = TracedRuntime::builder()
        .with_task_tracking(true)
        .with_tokio_instrumentation(false)
        .build_and_start(builder, writer)
        .unwrap();

    let handle = guard.handle();

    dial9_tokio_telemetry::telemetry::record_event(
        ProfilerOnlyMarker {
            timestamp_ns: dial9_tokio_telemetry::telemetry::clock_monotonic_ns(),
            request_count: 1,
        },
        &handle,
    );

    runtime.block_on(async {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let current_handle_enabled = runtime.block_on(async {
        dial9_tokio_telemetry::telemetry::TelemetryHandle::current().is_enabled()
    });

    drop(runtime);
    drop(guard);

    let sealed_path = dir.path().join("trace.0.bin");
    let reader = TraceReader::new(sealed_path.to_str().unwrap()).unwrap();
    let events = &reader.all_events;
    assert!(
        events.iter().any(|e| matches!(
            e,
            TelemetryEvent::Custom { name, .. } if name == "ProfilerOnlyMarker"
        )),
        "explicit record_event calls through guard.handle() should still be recorded"
    );

    assert!(
        events.iter().all(|e| !matches!(
            e,
            TelemetryEvent::PollStart { .. }
                | TelemetryEvent::PollEnd { .. }
                | TelemetryEvent::WorkerPark { .. }
                | TelemetryEvent::WorkerUnpark { .. }
                | TelemetryEvent::QueueSample { .. }
                | TelemetryEvent::TaskSpawn { .. }
                | TelemetryEvent::TaskTerminate { .. }
                | TelemetryEvent::WakeEvent { .. }
        )),
        "Tokio runtime events should not be recorded when Tokio instrumentation is disabled: {events:?}"
    );

    assert!(
        !current_handle_enabled,
        "TelemetryHandle::current() should remain inert when no Tokio hooks are installed"
    );
}

#[test]
fn tokio_instrumentation_disabled_clears_current_handle_from_previous_runtime() {
    let dir = tempfile::tempdir().unwrap();

    let trace_path = dir.path().join("instrumented.bin");
    let writer = RotatingWriter::single_file(&trace_path).unwrap();
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all();
    let (runtime, guard) = TracedRuntime::builder()
        .build_and_start(builder, writer)
        .unwrap();

    assert!(
        runtime.block_on(async {
            dial9_tokio_telemetry::telemetry::TelemetryHandle::current().is_enabled()
        }),
        "instrumented current_thread runtime should install TelemetryHandle::current()"
    );
    drop(runtime);
    drop(guard);

    let trace_path = dir.path().join("profiler-only.bin");
    let writer = RotatingWriter::single_file(&trace_path).unwrap();
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all();
    let (runtime, guard) = TracedRuntime::builder()
        .with_tokio_instrumentation(false)
        .build_and_start(builder, writer)
        .unwrap();

    assert!(
        !runtime.block_on(async {
            dial9_tokio_telemetry::telemetry::TelemetryHandle::current().is_enabled()
        }),
        "profiler-only runtime should not inherit a stale TelemetryHandle::current()"
    );
    drop(runtime);
    drop(guard);
}

#[test]
#[cfg(all(feature = "cpu-profiling", target_os = "linux"))]
fn tokio_instrumentation_can_be_disabled_without_disabling_cpu_profiling() {
    use dial9_tokio_telemetry::telemetry::CpuSampleSource;
    use dial9_tokio_telemetry::telemetry::cpu_profile::CpuProfilingConfig;

    let dir = tempfile::tempdir().unwrap();
    let trace_path = dir.path().join("trace.bin");
    let writer = RotatingWriter::single_file(&trace_path).unwrap();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(2).enable_all();

    let (runtime, guard) = TracedRuntime::builder()
        .with_cpu_profiling(CpuProfilingConfig::default())
        .with_tokio_instrumentation(false)
        .build_and_start(builder, writer)
        .unwrap();

    runtime.block_on(async {
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_millis(500) {
            std::hint::black_box(0u64.wrapping_mul(42));
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    });

    drop(runtime);
    drop(guard);

    let sealed_path = dir.path().join("trace.0.bin");
    let reader = TraceReader::new(sealed_path.to_str().unwrap()).unwrap();
    let events = &reader.all_events;

    assert!(
        events.iter().any(|e| matches!(
            e,
            TelemetryEvent::CpuSample {
                source: CpuSampleSource::CpuProfile,
                ..
            }
        )),
        "CPU profiling should still produce samples when Tokio instrumentation is disabled. Is perf_event_paranoid <= 2?"
    );
    assert!(
        events.iter().all(|e| !matches!(
            e,
            TelemetryEvent::PollStart { .. }
                | TelemetryEvent::PollEnd { .. }
                | TelemetryEvent::WorkerPark { .. }
                | TelemetryEvent::WorkerUnpark { .. }
                | TelemetryEvent::QueueSample { .. }
                | TelemetryEvent::TaskSpawn { .. }
                | TelemetryEvent::TaskTerminate { .. }
                | TelemetryEvent::WakeEvent { .. }
        )),
        "Tokio runtime events should not be recorded when Tokio instrumentation is disabled: {events:?}"
    );
}
