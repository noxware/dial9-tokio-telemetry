#![cfg(all(target_os = "linux", feature = "socket-accept-queues"))]

mod common;

use common::{BytesCapturingWriter, decode_all};
use dial9_tokio_telemetry::telemetry::analysis_events::Dial9Event;
use dial9_tokio_telemetry::telemetry::{SocketAcceptQueuesConfig, TracedRuntime};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

#[test]
fn traced_runtime_records_socket_accept_queue_snapshot() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let local_addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(local_addr).unwrap();

    let (writer, batches) = BytesCapturingWriter::new();
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(1).enable_all();
    let (runtime, guard) = TracedRuntime::builder()
        .with_socket_accept_queues(
            SocketAcceptQueuesConfig::builder()
                .sample_interval(Duration::ZERO)
                .build(),
        )
        .build_and_start_with_writer(builder, writer)
        .unwrap();

    drop(runtime);
    drop(guard);
    drop(client);
    drop(listener);

    let batches = batches.lock().unwrap();
    let events: Vec<Dial9Event> = decode_all(&batches);
    let snapshots: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Dial9Event::SocketAcceptQueueEvent(event) => Some(event),
            _ => None,
        })
        .collect();

    let snapshot = snapshots
        .iter()
        .find(|event| event.local_port == local_addr.port())
        .unwrap_or_else(|| panic!("expected snapshot for listener port {local_addr}"));

    assert!(snapshot.timestamp_ns > 0);
    assert!(snapshot.socket_cookie > 0);
    assert!(snapshot.socket_inode > 0);
    assert_eq!(snapshot.ip_version, 4);
    assert_eq!(snapshot.protocol, 6);
    assert_eq!(snapshot.local_addr, "127.0.0.1");
    assert_eq!(snapshot.local_port, local_addr.port());
    assert!(snapshot.pending_connections >= 1);
    assert!(snapshot.backlog_limit >= snapshot.pending_connections);
}

#[test]
fn traced_runtime_does_not_record_socket_accept_queues_by_default() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let local_addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(local_addr).unwrap();

    let (writer, batches) = BytesCapturingWriter::new();
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(1).enable_all();
    let (runtime, guard) = TracedRuntime::builder()
        .build_and_start_with_writer(builder, writer)
        .unwrap();

    drop(runtime);
    drop(guard);
    drop(client);
    drop(listener);

    let batches = batches.lock().unwrap();
    let events: Vec<Dial9Event> = decode_all(&batches);

    assert!(
        events
            .iter()
            .all(|event| !matches!(event, Dial9Event::SocketAcceptQueueEvent(_))),
        "socket accept queue snapshots should be opt-in"
    );
}
