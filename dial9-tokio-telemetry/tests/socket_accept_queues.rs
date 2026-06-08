mod common;

#[cfg(target_os = "linux")]
use common::{BytesCapturingWriter, decode_all};
#[cfg(target_os = "linux")]
use dial9_tokio_telemetry::telemetry::analysis_events::Dial9Event;
#[cfg(target_os = "linux")]
use dial9_tokio_telemetry::telemetry::{SocketAcceptQueuesConfig, TracedRuntime};
#[cfg(target_os = "linux")]
use std::net::{TcpListener, TcpStream};
#[cfg(target_os = "linux")]
use std::time::Duration;

#[cfg(target_os = "linux")]
#[test]
fn traced_runtime_records_socket_accept_queue_snapshot() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let local_port = listener.local_addr().unwrap().port();
    let _pending = TcpStream::connect(listener.local_addr().unwrap()).unwrap();

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

    let batches = batches.lock().unwrap();
    let events: Vec<Dial9Event> = decode_all(&batches);
    let snapshot = events.iter().find_map(|event| match event {
        Dial9Event::SocketAcceptQueueEvent(event) if event.local_port == local_port => Some(event),
        _ => None,
    });

    let snapshot = snapshot.expect("expected socket accept queue snapshot for listener");
    assert_eq!(snapshot.ip_version, 4);
    assert_eq!(snapshot.local_addr, [127, 0, 0, 1]);
    assert!(snapshot.socket_inode > 0);
    assert!(snapshot.pending_connections >= 1);
    assert!(snapshot.backlog_limit >= snapshot.pending_connections);
}

#[cfg(target_os = "linux")]
#[test]
fn traced_runtime_does_not_record_socket_accept_queues_by_default() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let _pending = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (writer, batches) = BytesCapturingWriter::new();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(1).enable_all();
    let (runtime, guard) = TracedRuntime::builder()
        .build_and_start_with_writer(builder, writer)
        .unwrap();

    drop(runtime);
    drop(guard);

    let batches = batches.lock().unwrap();
    let events: Vec<Dial9Event> = decode_all(&batches);

    assert!(
        events
            .iter()
            .all(|event| !matches!(event, Dial9Event::SocketAcceptQueueEvent(_))),
        "socket accept queue snapshots should be opt-in"
    );
}
