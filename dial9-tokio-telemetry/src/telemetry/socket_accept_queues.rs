//! TCP accept queue snapshots sampled from Linux sock_diag.

use std::time::Duration;

const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Configuration for TCP accept queue snapshots.
///
/// Built via `SocketAcceptQueuesConfig::builder()...build()` and enabled with
/// [`TracedRuntimeBuilder::with_socket_accept_queues`](crate::telemetry::TracedRuntimeBuilder::with_socket_accept_queues).
#[derive(Debug, Clone, bon::Builder)]
pub struct SocketAcceptQueuesConfig {
    /// Minimum time between sock_diag snapshots.
    #[builder(default = DEFAULT_SAMPLE_INTERVAL)]
    sample_interval: Duration,
}

impl Default for SocketAcceptQueuesConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl SocketAcceptQueuesConfig {
    /// Minimum time between sock_diag snapshots.
    pub fn sample_interval(&self) -> Duration {
        self.sample_interval
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::SocketAcceptQueuesConfig;
    use crate::rate_limit::rate_limited;
    use crate::telemetry::buffer::record_encodable_event;
    use crate::telemetry::events::clock_monotonic_ns;
    use crate::telemetry::format::SocketAcceptQueueEvent;
    use crate::telemetry::recorder::source::{FlushContext, Source};
    use netlink_packet_core::{
        NLM_F_DUMP, NLM_F_REQUEST, NetlinkHeader, NetlinkMessage, NetlinkPayload,
    };
    use netlink_packet_sock_diag::SockDiagMessage;
    use netlink_packet_sock_diag::constants::{AF_INET, AF_INET6, IPPROTO_TCP, TCP_LISTEN};
    use netlink_packet_sock_diag::inet::{
        ExtensionFlags, InetRequest, InetResponse, SocketId, StateFlags,
    };
    use netlink_sys::{Socket, SocketAddr, protocols::NETLINK_SOCK_DIAG};
    use std::collections::HashSet;
    use std::ffi::OsStr;
    use std::io;
    use std::net::IpAddr;
    use std::path::Path;
    use std::time::{Duration, Instant};

    /// Flush-thread source that samples TCP listen backlog state via sock_diag.
    #[derive(Debug)]
    pub(crate) struct SocketAcceptQueuesSource {
        config: SocketAcceptQueuesConfig,
        last_sample: Option<Instant>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SocketAcceptQueueSnapshot {
        socket_inode: u64,
        ip_version: u8,
        protocol: u8,
        local_addr: Vec<u8>,
        local_port: u16,
        pending_connections: u32,
        backlog_limit: u32,
    }

    impl SocketAcceptQueuesSource {
        pub(crate) fn new(config: SocketAcceptQueuesConfig) -> Self {
            Self {
                config,
                last_sample: None,
            }
        }
    }

    impl Source for SocketAcceptQueuesSource {
        fn flush(&mut self, ctx: &FlushContext<'_>) {
            let now = Instant::now();
            if let Some(last_sample) = self.last_sample
                && now.duration_since(last_sample) < self.config.sample_interval
            {
                return;
            }
            self.last_sample = Some(now);

            match collect_socket_accept_queues() {
                Ok(snapshots) => {
                    let timestamp_ns = clock_monotonic_ns();
                    for snapshot in snapshots {
                        let event = snapshot.into_event(timestamp_ns);
                        record_encodable_event(&event, ctx.collector, ctx.drain_epoch);
                    }
                }
                Err(e) => rate_limited!(Duration::from_secs(60), {
                    tracing::warn!("failed to read TCP accept queues via sock_diag: {e}");
                }),
            }
        }

        fn name(&self) -> &'static str {
            "socket_accept_queues"
        }
    }

    impl SocketAcceptQueueSnapshot {
        fn into_event(self, timestamp_ns: u64) -> SocketAcceptQueueEvent {
            SocketAcceptQueueEvent {
                timestamp_ns,
                socket_inode: self.socket_inode,
                ip_version: self.ip_version,
                protocol: self.protocol,
                local_addr: self.local_addr,
                local_port: self.local_port,
                pending_connections: self.pending_connections,
                backlog_limit: self.backlog_limit,
            }
        }
    }

    fn collect_socket_accept_queues() -> io::Result<Vec<SocketAcceptQueueSnapshot>> {
        let socket_inodes = current_process_socket_inodes()?;
        if socket_inodes.is_empty() {
            return Ok(Vec::new());
        }

        let mut socket = Socket::new(NETLINK_SOCK_DIAG)?;
        let _local_addr = socket.bind_auto()?;
        socket.connect(&SocketAddr::new(0, 0))?;

        let mut snapshots = Vec::new();
        dump_inet_accept_queues(&socket, AF_INET, 1, &socket_inodes, &mut snapshots)?;
        dump_inet_accept_queues(&socket, AF_INET6, 2, &socket_inodes, &mut snapshots)?;
        Ok(snapshots)
    }

    fn current_process_socket_inodes() -> io::Result<HashSet<u64>> {
        let mut inodes = HashSet::new();
        for entry in std::fs::read_dir("/proc/self/fd")? {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            let target = match std::fs::read_link(entry.path()) {
                Ok(target) => target,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            if let Some(inode) = parse_socket_inode(&target) {
                inodes.insert(inode);
            }
        }
        Ok(inodes)
    }

    fn parse_socket_inode(target: &Path) -> Option<u64> {
        parse_socket_inode_os_str(target.as_os_str())
    }

    fn parse_socket_inode_os_str(target: &OsStr) -> Option<u64> {
        let target = target.to_string_lossy();
        let inode = target.strip_prefix("socket:[")?.strip_suffix(']')?;
        inode.parse::<u64>().ok()
    }

    fn dump_inet_accept_queues(
        socket: &Socket,
        family: u8,
        sequence_number: u32,
        socket_inodes: &HashSet<u64>,
        snapshots: &mut Vec<SocketAcceptQueueSnapshot>,
    ) -> io::Result<()> {
        let mut header = NetlinkHeader::default();
        header.flags = NLM_F_REQUEST | NLM_F_DUMP;
        header.sequence_number = sequence_number;

        let socket_id = match family {
            AF_INET => SocketId::new_v4(),
            AF_INET6 => SocketId::new_v6(),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported inet family {family}"),
                ));
            }
        };
        let mut packet = NetlinkMessage::new(
            header,
            SockDiagMessage::InetRequest(InetRequest {
                family,
                protocol: IPPROTO_TCP,
                extensions: ExtensionFlags::empty(),
                states: StateFlags::LISTEN,
                socket_id,
            })
            .into(),
        );
        packet.finalize();

        let mut send_buffer = vec![0; packet.buffer_len()];
        packet.serialize(&mut send_buffer);
        socket.send(&send_buffer, 0)?;

        loop {
            let (receive_buffer, _sender) = socket.recv_from_full()?;
            let mut offset = 0;
            while offset < receive_buffer.len() {
                let packet =
                    NetlinkMessage::<SockDiagMessage>::deserialize(&receive_buffer[offset..])
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

                if packet.header.length == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "sock_diag returned zero-length netlink message",
                    ));
                }

                let is_done = handle_netlink_payload(packet.payload, socket_inodes, snapshots)?;
                if is_done {
                    return Ok(());
                }

                offset += packet.header.length as usize;
            }
        }
    }

    fn handle_netlink_payload(
        payload: NetlinkPayload<SockDiagMessage>,
        socket_inodes: &HashSet<u64>,
        snapshots: &mut Vec<SocketAcceptQueueSnapshot>,
    ) -> io::Result<bool> {
        match payload {
            NetlinkPayload::InnerMessage(SockDiagMessage::InetResponse(response)) => {
                if let Some(snapshot) = snapshot_from_response(&response, socket_inodes) {
                    snapshots.push(snapshot);
                }
                Ok(false)
            }
            NetlinkPayload::Done(_) => Ok(true),
            NetlinkPayload::Error(error) => Err(error.into()),
            NetlinkPayload::Noop | NetlinkPayload::Overrun(_) => Ok(false),
            _ => Ok(false),
        }
    }

    fn snapshot_from_response(
        response: &InetResponse,
        socket_inodes: &HashSet<u64>,
    ) -> Option<SocketAcceptQueueSnapshot> {
        let header = &response.header;
        if header.state != TCP_LISTEN {
            return None;
        }

        let socket_inode = u64::from(header.inode);
        if !socket_inodes.contains(&socket_inode) {
            return None;
        }

        let (ip_version, local_addr) = match header.socket_id.source_address {
            IpAddr::V4(addr) => (4, addr.octets().to_vec()),
            IpAddr::V6(addr) => (6, addr.octets().to_vec()),
        };

        Some(SocketAcceptQueueSnapshot {
            socket_inode,
            ip_version,
            protocol: IPPROTO_TCP,
            local_addr,
            local_port: header.socket_id.source_port,
            pending_connections: header.recv_queue,
            backlog_limit: header.send_queue,
        })
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parses_proc_fd_socket_symlink() {
            assert_eq!(
                parse_socket_inode_os_str(OsStr::new("socket:[12345]")),
                Some(12345)
            );
            assert_eq!(parse_socket_inode_os_str(OsStr::new("/tmp/file")), None);
        }

        #[test]
        fn collect_socket_accept_queues_reports_pending_tcp_listener() {
            let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let local_port = listener.local_addr().unwrap().port();
            let _pending = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();

            let snapshots = collect_socket_accept_queues().unwrap();
            let snapshot = snapshots
                .iter()
                .find(|snapshot| snapshot.local_port == local_port)
                .expect("expected snapshot for test listener");

            assert_eq!(snapshot.ip_version, 4);
            assert_eq!(snapshot.local_addr, [127, 0, 0, 1]);
            assert!(snapshot.socket_inode > 0);
            assert!(snapshot.pending_connections >= 1);
            assert!(snapshot.backlog_limit >= snapshot.pending_connections);
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::SocketAcceptQueuesSource;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sample_interval_is_1s() {
        assert_eq!(
            SocketAcceptQueuesConfig::default().sample_interval(),
            DEFAULT_SAMPLE_INTERVAL
        );
    }
}
