//! Socket accept queue snapshots sampled from Linux sock_diag.

use std::time::Duration;

const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Configuration for socket accept queue sampling.
///
/// Built via `SocketAcceptQueuesConfig::builder()...build()` and enabled with
/// [`TracedRuntimeBuilder::with_socket_accept_queues`](crate::telemetry::TracedRuntimeBuilder::with_socket_accept_queues).
#[derive(Debug, Clone, bon::Builder)]
pub struct SocketAcceptQueuesConfig {
    /// Minimum time between samples.
    #[builder(default = DEFAULT_SAMPLE_INTERVAL)]
    sample_interval: Duration,
}

impl Default for SocketAcceptQueuesConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl SocketAcceptQueuesConfig {
    /// Minimum time between samples.
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
        NLM_F_DUMP, NLM_F_REQUEST, NetlinkBuffer, NetlinkMessage, NetlinkPayload,
    };
    use netlink_packet_sock_diag::inet::{ExtensionFlags, InetRequest, SocketId, StateFlags};
    use netlink_packet_sock_diag::{AF_INET, AF_INET6, IPPROTO_TCP, SockDiagMessage, TCP_LISTEN};
    use netlink_sys::{Socket, SocketAddr, protocols::NETLINK_SOCK_DIAG};
    use std::collections::HashSet;
    use std::fs;
    use std::io;
    use std::net::IpAddr;
    use std::path::Path;
    use std::time::{Duration, Instant};

    /// Flush-thread source that samples TCP listener accept queue depth.
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
        local_addr: IpAddr,
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
                        record_encodable_event(
                            &snapshot.into_event(timestamp_ns),
                            ctx.collector,
                            ctx.drain_epoch,
                        );
                    }
                }
                Err(e) => rate_limited!(Duration::from_secs(60), {
                    tracing::warn!("failed to read socket accept queues via sock_diag: {e}");
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
                local_addr: self.local_addr.to_string(),
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
        let kernel_addr = SocketAddr::new(0, 0);
        socket.connect(&kernel_addr)?;

        let mut snapshots = Vec::new();
        dump_tcp_listeners(&socket, AF_INET, 1, &socket_inodes, &mut snapshots)?;
        dump_tcp_listeners(&socket, AF_INET6, 2, &socket_inodes, &mut snapshots)?;
        Ok(snapshots)
    }

    fn dump_tcp_listeners(
        socket: &Socket,
        family: u8,
        sequence_number: u32,
        socket_inodes: &HashSet<u64>,
        snapshots: &mut Vec<SocketAcceptQueueSnapshot>,
    ) -> io::Result<()> {
        let socket_id = match family {
            AF_INET => SocketId::new_v4(),
            AF_INET6 => SocketId::new_v6(),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported inet socket family {family}"),
                ));
            }
        };

        let request = InetRequest {
            family,
            protocol: IPPROTO_TCP,
            extensions: ExtensionFlags::empty(),
            states: StateFlags::LISTEN,
            socket_id,
        };
        let mut message = NetlinkMessage::from(SockDiagMessage::InetRequest(request));
        message.header.flags = NLM_F_REQUEST | NLM_F_DUMP;
        message.header.sequence_number = sequence_number;
        message.finalize();

        let mut request_bytes = vec![0; message.buffer_len()];
        message.serialize(&mut request_bytes);
        let sent = socket.send(&request_bytes, 0)?;
        if sent != request_bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "short netlink sock_diag request write: sent {sent} of {} bytes",
                    request_bytes.len()
                ),
            ));
        }

        let mut response_bytes = vec![0; 32 * 1024];
        loop {
            let mut response_slice = &mut response_bytes[..];
            let received = socket.recv(&mut response_slice, 0)?;
            if received == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "sock_diag netlink socket returned an empty response",
                ));
            }

            let done = parse_response_datagram(
                &response_bytes[..received],
                sequence_number,
                socket_inodes,
                snapshots,
            )?;
            if done {
                return Ok(());
            }
        }
    }

    fn parse_response_datagram(
        bytes: &[u8],
        sequence_number: u32,
        socket_inodes: &HashSet<u64>,
        snapshots: &mut Vec<SocketAcceptQueueSnapshot>,
    ) -> io::Result<bool> {
        let mut offset = 0;
        let mut done = false;

        while offset < bytes.len() {
            let remaining = &bytes[offset..];
            let packet = NetlinkBuffer::new_checked(&remaining).map_err(decode_error)?;
            let packet_len = packet.length() as usize;
            let message = NetlinkMessage::<SockDiagMessage>::deserialize(&remaining[..packet_len])
                .map_err(decode_error)?;

            if message.header.sequence_number == sequence_number {
                match message.payload {
                    NetlinkPayload::Done(_) => done = true,
                    NetlinkPayload::Error(error) => {
                        if error.code.is_some() {
                            return Err(error.into());
                        }
                    }
                    NetlinkPayload::InnerMessage(SockDiagMessage::InetResponse(response)) => {
                        if let Some(snapshot) = snapshot_from_response(&response, socket_inodes) {
                            snapshots.push(snapshot);
                        }
                    }
                    _ => {}
                }
            }

            offset += nlmsg_align(packet_len);
        }

        Ok(done)
    }

    fn snapshot_from_response(
        response: &netlink_packet_sock_diag::inet::InetResponse,
        socket_inodes: &HashSet<u64>,
    ) -> Option<SocketAcceptQueueSnapshot> {
        let header = &response.header;
        let socket_inode = u64::from(header.inode);
        if header.state != TCP_LISTEN || !socket_inodes.contains(&socket_inode) {
            return None;
        }

        Some(SocketAcceptQueueSnapshot {
            socket_inode,
            ip_version: match header.family {
                AF_INET => 4,
                AF_INET6 => 6,
                _ => return None,
            },
            protocol: IPPROTO_TCP,
            local_addr: header.socket_id.source_address,
            local_port: header.socket_id.source_port,
            pending_connections: header.recv_queue,
            backlog_limit: header.send_queue,
        })
    }

    fn current_process_socket_inodes() -> io::Result<HashSet<u64>> {
        let mut inodes = HashSet::new();
        for entry in fs::read_dir("/proc/self/fd")? {
            let entry = entry?;
            match fs::read_link(entry.path()) {
                Ok(target) => {
                    if let Some(inode) = parse_socket_inode(&target)? {
                        inodes.insert(inode);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(inodes)
    }

    fn parse_socket_inode(path: &Path) -> io::Result<Option<u64>> {
        let Some(text) = path.to_str() else {
            return Ok(None);
        };
        let Some(inode) = text
            .strip_prefix("socket:[")
            .and_then(|value| value.strip_suffix(']'))
        else {
            return Ok(None);
        };

        inode.parse::<u64>().map(Some).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid /proc/self/fd socket inode {inode:?}: {e}"),
            )
        })
    }

    fn decode_error(error: impl std::fmt::Display) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
    }

    fn nlmsg_align(len: usize) -> usize {
        (len + 3) & !3
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parse_socket_inode_extracts_socket_symlink_inode() {
            assert_eq!(
                parse_socket_inode(Path::new("socket:[12345]")).unwrap(),
                Some(12345)
            );
        }

        #[test]
        fn parse_socket_inode_ignores_non_socket_symlink() {
            assert_eq!(parse_socket_inode(Path::new("/dev/null")).unwrap(), None);
        }

        #[test]
        fn default_sample_interval_is_one_second() {
            assert_eq!(
                SocketAcceptQueuesConfig::default().sample_interval(),
                Duration::from_secs(1)
            );
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux::SocketAcceptQueuesSource;

#[cfg(not(target_os = "linux"))]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sample_interval_is_one_second() {
        assert_eq!(
            SocketAcceptQueuesConfig::default().sample_interval(),
            Duration::from_secs(1)
        );
    }
}
