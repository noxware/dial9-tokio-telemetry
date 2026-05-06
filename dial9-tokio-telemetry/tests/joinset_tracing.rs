//! Integration tests for the JoinSet-friendly tracing API:
//! - `TelemetryHandle::trace` / `RuntimeTelemetryHandle::trace`
//! - `TelemetryHandle::with_instrumented_spawn` / mirror on runtime handle

mod common;

use dial9_tokio_telemetry::analysis_unstable::TraceReader;
use dial9_tokio_telemetry::telemetry::{
    RotatingWriter, TaskId, TelemetryEvent, TelemetryGuard, TraceWriter, TracedRuntime,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

/// Standard 2-worker multi_thread runtime with task tracking enabled.
fn build_traced_runtime<W: TraceWriter + 'static>(writer: W) -> (Runtime, TelemetryGuard) {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(2).enable_all();
    TracedRuntime::builder()
        .with_task_tracking(true)
        .build_and_start(builder, writer)
        .unwrap()
}

/// `set.spawn(handle.trace(fut))` produces `WakeEvent`s for the spawned
/// task — the same as `handle.spawn(fut)` would.
#[test]
fn wake_tracking_via_joinset_trace() {
    let (writer, events) = common::CapturingWriter::new();
    let (runtime, guard) = build_traced_runtime(writer);

    let handle = guard.handle();
    let spawned_id: Arc<Mutex<Option<TaskId>>> = Arc::new(Mutex::new(None));
    let id_w = spawned_id.clone();

    runtime.block_on(async move {
        let mut set: JoinSet<()> = JoinSet::new();
        // `yield_now().await` self-wakes through the active waker, which
        // here is our `Traced` waker — so a `WakeEvent` fires without
        // depending on cross-task scheduling order.
        set.spawn(handle.trace(async move {
            *id_w.lock().unwrap() = tokio::task::try_id().map(TaskId::from);
            tokio::task::yield_now().await;
        }));
        while set.join_next().await.is_some() {}
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    drop(runtime);
    drop(guard);

    let events = events.lock().unwrap();
    let expected = spawned_id.lock().unwrap().expect("task id captured");
    let saw_wake = events.iter().any(|e| {
        matches!(e, TelemetryEvent::WakeEvent { woken_task_id, .. } if *woken_task_id == expected)
    });
    assert!(saw_wake, "expected WakeEvent for joinset task {expected:?}");
}

/// Awaiting `trace(fut)` directly in `block_on` runs outside any Tokio task,
/// so there is no task ID to attach wake tracking to.
#[test]
fn trace_outside_task_context_skips_wake_tracking() {
    let (writer, events) = common::CapturingWriter::new();
    let (runtime, guard) = build_traced_runtime(writer);

    let handle = guard.handle();
    let observed_id: Arc<Mutex<Option<Option<TaskId>>>> = Arc::new(Mutex::new(None));
    let id_w = observed_id.clone();

    runtime.block_on(async move {
        let result = handle
            .trace(async move {
                *id_w.lock().unwrap() = Some(tokio::task::try_id().map(TaskId::from));
                tokio::task::yield_now().await;
                7u32
            })
            .await;
        assert_eq!(result, 7);
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    drop(runtime);
    drop(guard);

    assert_eq!(*observed_id.lock().unwrap(), Some(None));
    let saw_wake = events
        .lock()
        .unwrap()
        .iter()
        .any(|e| matches!(e, TelemetryEvent::WakeEvent { .. }));
    assert!(
        !saw_wake,
        "expected no WakeEvent when trace() is awaited outside a task"
    );
}

/// `with_instrumented_spawn` flips the TaskSpawn `instrumented` flag for
/// any spawn that happens inside the closure, AND because the closure body
/// lives in user code, `tokio::spawn`'s `#[track_caller]` resolves
/// `spawn_loc` to the user's file (NOT to the library).
#[test]
fn with_instrumented_spawn_marks_taskspawn_and_preserves_caller() {
    let dir = tempfile::tempdir().unwrap();
    let trace_path = dir.path().join("trace.bin");
    let writer = RotatingWriter::single_file(&trace_path).unwrap();
    let (runtime, guard) = build_traced_runtime(writer);

    let handle = guard.handle();

    runtime.block_on(async move {
        // Inside the closure: marked instrumented, caller = this file.
        let join = handle.with_instrumented_spawn(|| tokio::spawn(async {}));
        join.await.unwrap();

        // Outside the closure: NOT instrumented.
        tokio::spawn(async {}).await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    drop(runtime);
    drop(guard);

    let sealed = dir.path().join("trace.0.bin");
    let reader = TraceReader::new(sealed.to_str().unwrap()).unwrap();

    let mut instrumented_user_loc = 0;
    let mut raw = 0;
    for event in &reader.all_events {
        if let TelemetryEvent::TaskSpawn {
            spawn_loc,
            instrumented,
            ..
        } = event
        {
            match instrumented {
                Some(true) => {
                    let loc = reader
                        .spawn_locations
                        .get(spawn_loc)
                        .expect("spawn_loc should resolve");
                    assert!(
                        loc.contains("joinset_tracing.rs"),
                        "instrumented spawn caller should resolve to user code, got {loc}"
                    );
                    instrumented_user_loc += 1;
                }
                Some(false) => raw += 1,
                None => {}
            }
        }
    }
    assert_eq!(
        instrumented_user_loc, 1,
        "expected 1 instrumented TaskSpawn pointing to joinset_tracing.rs"
    );
    assert!(raw >= 1, "expected at least 1 raw TaskSpawn, got {raw}");
}

/// Composing `with_instrumented_spawn` + `trace` + `JoinSet::spawn`
/// emits wake events AND marks `TaskSpawn` instrumented. Caller location
/// resolves to the user's file because the closure body lives there.
#[test]
fn composed_joinset_spawn_emits_wakes_and_marks_instrumented() {
    let dir = tempfile::tempdir().unwrap();
    let trace_path = dir.path().join("trace.bin");
    let writer = RotatingWriter::single_file(&trace_path).unwrap();
    let (runtime, guard) = build_traced_runtime(writer);

    let handle = guard.handle();
    let spawned_id: Arc<Mutex<Option<TaskId>>> = Arc::new(Mutex::new(None));
    let id_w = spawned_id.clone();

    runtime.block_on(async move {
        let mut set: JoinSet<()> = JoinSet::new();
        handle.with_instrumented_spawn(|| {
            set.spawn(handle.trace(async move {
                *id_w.lock().unwrap() = tokio::task::try_id().map(TaskId::from);
                tokio::task::yield_now().await;
            }))
        });
        while set.join_next().await.is_some() {}
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    drop(runtime);
    drop(guard);

    let sealed = dir.path().join("trace.0.bin");
    let reader = TraceReader::new(sealed.to_str().unwrap()).unwrap();
    let expected = spawned_id.lock().unwrap().expect("task id captured");

    let saw_wake = reader.runtime_events.iter().any(|e| {
        matches!(e, TelemetryEvent::WakeEvent { woken_task_id, .. } if *woken_task_id == expected)
    });
    assert!(saw_wake, "expected WakeEvent for traced joinset task");

    let mut found_instrumented = false;
    for event in &reader.all_events {
        if let TelemetryEvent::TaskSpawn {
            spawn_loc,
            instrumented: Some(true),
            ..
        } = event
        {
            let loc = reader
                .spawn_locations
                .get(spawn_loc)
                .expect("spawn_loc should resolve");
            if loc.contains("joinset_tracing.rs") {
                found_instrumented = true;
                break;
            }
        }
    }
    assert!(
        found_instrumented,
        "expected an instrumented TaskSpawn pointing to joinset_tracing.rs"
    );
}

/// Composing the `RuntimeTelemetryHandle` API with `JoinSet::spawn_on`
/// targets the correct runtime even when called from outside any runtime
/// context.
#[test]
fn runtime_handle_composed_joinset_targets_correct_runtime() {
    use dial9_tokio_telemetry::telemetry::{NullWriter, TelemetryCore};

    let guard = TelemetryCore::builder().writer(NullWriter).build().unwrap();
    guard.enable();

    let mut builder_a = tokio::runtime::Builder::new_multi_thread();
    builder_a.worker_threads(1).enable_all().thread_name("rt-a");
    let (rt_a, handle_a) = guard.trace_runtime("a").build(builder_a).unwrap();

    let mut builder_b = tokio::runtime::Builder::new_multi_thread();
    builder_b.worker_threads(1).enable_all().thread_name("rt-b");
    let (rt_b, handle_b) = guard.trace_runtime("b").build(builder_b).unwrap();

    let mut set_a: JoinSet<String> = JoinSet::new();
    handle_a.with_instrumented_spawn(|| {
        set_a.spawn_on(
            handle_a.trace(async {
                tokio::task::yield_now().await;
                std::thread::current().name().unwrap_or("?").to_string()
            }),
            rt_a.handle(),
        );
    });

    let mut set_b: JoinSet<String> = JoinSet::new();
    handle_b.with_instrumented_spawn(|| {
        set_b.spawn_on(
            handle_b.trace(async {
                tokio::task::yield_now().await;
                std::thread::current().name().unwrap_or("?").to_string()
            }),
            rt_b.handle(),
        );
    });

    let name_a = rt_a.block_on(async move { set_a.join_next().await.unwrap().unwrap() });
    let name_b = rt_b.block_on(async move { set_b.join_next().await.unwrap().unwrap() });

    assert!(name_a.starts_with("rt-a"), "got {name_a}");
    assert!(name_b.starts_with("rt-b"), "got {name_b}");

    drop(rt_a);
    drop(rt_b);
    let _ = guard.graceful_shutdown(Duration::from_secs(1));
}
