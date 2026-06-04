mod common;

#[cfg(target_os = "linux")]
use common::{BytesCapturingWriter, decode_all};
#[cfg(target_os = "linux")]
use dial9_tokio_telemetry::telemetry::analysis_events::Dial9Event;
#[cfg(target_os = "linux")]
use dial9_tokio_telemetry::telemetry::{SocketAcceptQueuesConfig, TracedRuntime};
#[cfg(target_os = "linux")]
use std::time::Duration;

#[cfg(target_os = "linux")]
#[test]
fn traced_runtime_records_socket_accept_queue_for_process_listener() {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("test listener should bind to loopback");
    let addr = listener
        .local_addr()
        .expect("test listener should have a local address");
    let pending_client = std::net::TcpStream::connect(addr)
        .expect("client connection should enter the listener accept queue");

    let (writer, batches) = BytesCapturingWriter::new();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(1).enable_all();
    let (runtime, guard) = TracedRuntime::builder()
        .with_socket_accept_queues(SocketAcceptQueuesConfig::default())
        .build_and_start_with_writer(builder, writer)
        .expect("traced runtime should build");

    drop(runtime);
    guard
        .graceful_shutdown(Duration::from_secs(5))
        .expect("telemetry should shut down cleanly");
    drop(pending_client);
    drop(listener);

    let batches = batches.lock().unwrap();
    let events: Vec<Dial9Event> = decode_all(&batches);
    let queue_events: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Dial9Event::SocketAcceptQueueEvent(event) => Some(event),
            _ => None,
        })
        .collect();

    assert!(
        queue_events.iter().any(|event| {
            event.local_port == addr.port()
                && event.pending_connections >= 1
                && event.backlog_limit >= event.pending_connections
        }),
        "expected a socket accept queue event for listener {addr}; got {queue_events:?}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn traced_runtime_does_not_record_socket_accept_queues_by_default() {
    let (writer, batches) = BytesCapturingWriter::new();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(1).enable_all();
    let (runtime, guard) = TracedRuntime::builder()
        .build_and_start_with_writer(builder, writer)
        .expect("traced runtime should build");

    drop(runtime);
    guard
        .graceful_shutdown(Duration::from_secs(5))
        .expect("telemetry should shut down cleanly");

    let batches = batches.lock().unwrap();
    let events: Vec<Dial9Event> = decode_all(&batches);

    assert!(
        events
            .iter()
            .all(|event| !matches!(event, Dial9Event::SocketAcceptQueueEvent(_))),
        "socket accept queues should be opt-in"
    );
}
