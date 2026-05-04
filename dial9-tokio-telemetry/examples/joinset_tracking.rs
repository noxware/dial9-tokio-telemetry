//! Spawn traced futures into a `tokio::task::JoinSet`.
//!
//! `TelemetryHandle::spawn` returns a `JoinHandle`, so it cannot feed a
//! `JoinSet` directly. `spawn_in_joinset` bridges the two: tasks are tracked
//! the same as `handle.spawn(...)` would be.
//!
//! Usage:
//!   cargo run --example joinset_tracking
//!
//! Inspect the trace afterwards:
//!   cargo run --example analyze_trace -- joinset_tracking_trace.0.bin

use dial9_tokio_telemetry::config::{Dial9Config, Dial9ConfigBuilder};
use dial9_tokio_telemetry::telemetry::TelemetryHandle;
use tokio::task::JoinSet;

fn my_config() -> Dial9Config {
    Dial9ConfigBuilder::new(
        "joinset_tracking_trace.bin",
        64 * 1024 * 1024,
        256 * 1024 * 1024,
    )
    .with_tokio(|t| {
        t.worker_threads(2);
    })
    .with_runtime(|r| r.with_task_tracking(true))
    .build()
}

async fn work(id: usize) -> usize {
    tokio::task::yield_now().await;
    id
}

#[dial9_tokio_telemetry::main(config = my_config)]
async fn main() {
    let handle = TelemetryHandle::current();
    let mut set: JoinSet<usize> = JoinSet::new();

    for i in 0..8 {
        // Roughly equivalent to:
        //   handle.with_instrumented_spawn(|| set.spawn(handle.trace(work(i))));
        handle.spawn_in_joinset(&mut set, work(i));
    }

    let mut total = 0;
    while let Some(res) = set.join_next().await {
        total += res.unwrap();
    }
    println!("sum = {total}");
    println!("Trace written to joinset_tracking_trace.*.bin");
}
