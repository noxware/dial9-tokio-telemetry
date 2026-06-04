//! TCP listen socket accept queues sampled from the operating system.

use std::time::Duration;

const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Configuration for TCP listen socket accept queue sampling.
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
    use std::collections::HashSet;
    use std::fs;
    use std::io;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::os::fd::{FromRawFd, OwnedFd};
    use std::path::Path;
    use std::time::{Duration, Instant};

    const NETLINK_HEADER_LEN: usize = 16;
    const INET_DIAG_REQ_V2_LEN: usize = 56;
    const INET_DIAG_MSG_LEN: usize = 72;
    const SOCK_DIAG_BY_FAMILY: u16 = 20;
    const NLMSG_ERROR: u16 = 2;
    const NLMSG_DONE: u16 = 3;
    const NLM_F_REQUEST: u16 = 0x1;
    const NLM_F_DUMP: u16 = 0x300;
    const TCP_LISTEN: u8 = 10;
    const TCPF_LISTEN: u32 = 1 << TCP_LISTEN;
    const RESPONSE_BUFFER_LEN: usize = 256 * 1024;

    #[repr(C)]
    struct SockaddrNl {
        nl_family: libc::sa_family_t,
        nl_pad: u16,
        nl_pid: u32,
        nl_groups: u32,
    }

    /// Flush-thread source that samples TCP listen accept queues via sock_diag.
    #[derive(Debug)]
    pub(crate) struct SocketAcceptQueuesSource {
        config: SocketAcceptQueuesConfig,
        last_sample: Option<Instant>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SocketAcceptQueueSnapshot {
        address_family: u8,
        protocol: u8,
        local_addr: String,
        local_port: u16,
        socket_inode: u64,
        pending_connections: u64,
        backlog_limit: u64,
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

            match read_socket_accept_queues() {
                Ok(snapshots) => {
                    let timestamp_ns = clock_monotonic_ns();
                    for snapshot in snapshots {
                        let event = snapshot.into_event(timestamp_ns);
                        record_encodable_event(&event, ctx.collector, ctx.drain_epoch);
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
                address_family: self.address_family,
                protocol: self.protocol,
                local_addr: self.local_addr,
                local_port: self.local_port,
                socket_inode: self.socket_inode,
                pending_connections: self.pending_connections,
                backlog_limit: self.backlog_limit,
            }
        }
    }

    fn read_socket_accept_queues() -> io::Result<Vec<SocketAcceptQueueSnapshot>> {
        let process_socket_inodes = read_process_socket_inodes()?;
        if process_socket_inodes.is_empty() {
            return Ok(Vec::new());
        }

        let mut socket = NetlinkSocket::open()?;
        let mut snapshots = Vec::new();
        socket.dump_tcp_listeners(libc::AF_INET as u8, &process_socket_inodes, &mut snapshots)?;
        socket.dump_tcp_listeners(libc::AF_INET6 as u8, &process_socket_inodes, &mut snapshots)?;
        Ok(snapshots)
    }

    fn read_process_socket_inodes() -> io::Result<HashSet<u64>> {
        let mut inodes = HashSet::new();
        for entry in fs::read_dir("/proc/self/fd")? {
            let entry = entry?;
            match fs::read_link(entry.path()) {
                Ok(target) => {
                    if let Some(inode) = parse_socket_inode(&target) {
                        inodes.insert(inode);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(inodes)
    }

    fn parse_socket_inode(target: &Path) -> Option<u64> {
        target
            .to_str()?
            .strip_prefix("socket:[")?
            .strip_suffix(']')?
            .parse()
            .ok()
    }

    struct NetlinkSocket {
        fd: OwnedFd,
        seq: u32,
    }

    impl NetlinkSocket {
        fn open() -> io::Result<Self> {
            // SAFETY: socket() has no Rust memory-safety requirements. The
            // returned fd is checked and then owned by OwnedFd.
            let fd = unsafe {
                libc::socket(
                    libc::AF_NETLINK,
                    libc::SOCK_RAW | libc::SOCK_CLOEXEC,
                    libc::NETLINK_SOCK_DIAG,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }

            // SAFETY: fd is a newly-created, valid file descriptor and becomes
            // uniquely owned by OwnedFd here.
            let fd = unsafe { OwnedFd::from_raw_fd(fd) };
            let addr = SockaddrNl {
                nl_family: libc::AF_NETLINK as libc::sa_family_t,
                nl_pad: 0,
                nl_pid: 0,
                nl_groups: 0,
            };

            // SAFETY: addr points to a valid sockaddr_nl-compatible value for
            // the duration of the call, and fd is a valid netlink socket.
            let rc = unsafe {
                libc::connect(
                    std::os::fd::AsRawFd::as_raw_fd(&fd),
                    (&addr as *const SockaddrNl).cast::<libc::sockaddr>(),
                    std::mem::size_of::<SockaddrNl>() as libc::socklen_t,
                )
            };
            if rc != 0 {
                return Err(io::Error::last_os_error());
            }

            Ok(Self { fd, seq: 0 })
        }

        fn dump_tcp_listeners(
            &mut self,
            family: u8,
            process_socket_inodes: &HashSet<u64>,
            snapshots: &mut Vec<SocketAcceptQueueSnapshot>,
        ) -> io::Result<()> {
            self.seq = self.seq.wrapping_add(1);
            let request = inet_diag_request(family, self.seq);
            send_all(std::os::fd::AsRawFd::as_raw_fd(&self.fd), &request)?;

            let mut buf = vec![0_u8; RESPONSE_BUFFER_LEN];
            loop {
                let len = recv_response(std::os::fd::AsRawFd::as_raw_fd(&self.fd), &mut buf)?;
                if parse_netlink_response(&buf[..len], self.seq, process_socket_inodes, snapshots)?
                {
                    return Ok(());
                }
            }
        }
    }

    fn inet_diag_request(family: u8, seq: u32) -> [u8; NETLINK_HEADER_LEN + INET_DIAG_REQ_V2_LEN] {
        let mut request = [0_u8; NETLINK_HEADER_LEN + INET_DIAG_REQ_V2_LEN];
        let request_len = request.len() as u32;
        write_u32_ne(&mut request[0..4], request_len);
        write_u16_ne(&mut request[4..6], SOCK_DIAG_BY_FAMILY);
        write_u16_ne(&mut request[6..8], NLM_F_REQUEST | NLM_F_DUMP);
        write_u32_ne(&mut request[8..12], seq);
        write_u32_ne(&mut request[12..16], 0);

        request[16] = family;
        request[17] = libc::IPPROTO_TCP as u8;
        request[18] = 0;
        request[19] = 0;
        write_u32_ne(&mut request[20..24], TCPF_LISTEN);

        // inet_diag_sockid.cookie = {-1, -1}: wildcard socket cookie.
        request[64..72].fill(0xff);
        request
    }

    fn send_all(fd: libc::c_int, request: &[u8]) -> io::Result<()> {
        loop {
            // SAFETY: request points to request.len() initialized bytes, and fd
            // is a valid connected netlink socket.
            let rc = unsafe { libc::send(fd, request.as_ptr().cast(), request.len(), 0) };
            if rc < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
            if rc as usize != request.len() {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "short sock_diag request write",
                ));
            }
            return Ok(());
        }
    }

    fn recv_response(fd: libc::c_int, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            // SAFETY: buf points to writable memory for buf.len() bytes, and fd
            // is a valid connected netlink socket.
            let rc = unsafe { libc::recv(fd, buf.as_mut_ptr().cast(), buf.len(), 0) };
            if rc < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
            if rc == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "sock_diag socket closed",
                ));
            }
            return Ok(rc as usize);
        }
    }

    fn parse_netlink_response(
        bytes: &[u8],
        seq: u32,
        process_socket_inodes: &HashSet<u64>,
        snapshots: &mut Vec<SocketAcceptQueueSnapshot>,
    ) -> io::Result<bool> {
        let mut offset = 0;
        while offset + NETLINK_HEADER_LEN <= bytes.len() {
            let nlmsg_len = read_u32_ne(&bytes[offset..offset + 4]) as usize;
            if nlmsg_len < NETLINK_HEADER_LEN || offset + nlmsg_len > bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "malformed sock_diag netlink message length",
                ));
            }

            let nlmsg_type = read_u16_ne(&bytes[offset + 4..offset + 6]);
            let nlmsg_seq = read_u32_ne(&bytes[offset + 8..offset + 12]);
            let payload = &bytes[offset + NETLINK_HEADER_LEN..offset + nlmsg_len];
            if nlmsg_seq == seq {
                match nlmsg_type {
                    NLMSG_DONE => return Ok(true),
                    NLMSG_ERROR => return parse_netlink_error(payload),
                    SOCK_DIAG_BY_FAMILY => {
                        if let Some(snapshot) = parse_inet_diag_msg(payload)?
                            && process_socket_inodes.contains(&snapshot.socket_inode)
                        {
                            snapshots.push(snapshot);
                        }
                    }
                    _ => {}
                }
            }

            offset += align_netlink_len(nlmsg_len);
        }
        Ok(false)
    }

    fn parse_netlink_error(payload: &[u8]) -> io::Result<bool> {
        if payload.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated sock_diag netlink error",
            ));
        }
        let error = i32::from_ne_bytes(payload[0..4].try_into().expect("slice length checked"));
        if error == 0 {
            return Ok(false);
        }
        Err(io::Error::from_raw_os_error(-error))
    }

    fn parse_inet_diag_msg(payload: &[u8]) -> io::Result<Option<SocketAcceptQueueSnapshot>> {
        if payload.len() < INET_DIAG_MSG_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated inet_diag response",
            ));
        }
        if payload[1] != TCP_LISTEN {
            return Ok(None);
        }

        let family = payload[0];
        let local_port =
            u16::from_be_bytes(payload[4..6].try_into().expect("slice length checked"));
        let local_addr = match i32::from(family) {
            libc::AF_INET => {
                Ipv4Addr::new(payload[8], payload[9], payload[10], payload[11]).to_string()
            }
            libc::AF_INET6 => {
                let addr: [u8; 16] = payload[8..24].try_into().expect("slice length checked");
                Ipv6Addr::from(addr).to_string()
            }
            _ => return Ok(None),
        };

        Ok(Some(SocketAcceptQueueSnapshot {
            address_family: family,
            protocol: libc::IPPROTO_TCP as u8,
            local_addr,
            local_port,
            pending_connections: u64::from(read_u32_ne(&payload[56..60])),
            backlog_limit: u64::from(read_u32_ne(&payload[60..64])),
            socket_inode: u64::from(read_u32_ne(&payload[68..72])),
        }))
    }

    fn align_netlink_len(len: usize) -> usize {
        (len + 3) & !3
    }

    fn write_u16_ne(dst: &mut [u8], value: u16) {
        dst.copy_from_slice(&value.to_ne_bytes());
    }

    fn write_u32_ne(dst: &mut [u8], value: u32) {
        dst.copy_from_slice(&value.to_ne_bytes());
    }

    fn read_u16_ne(src: &[u8]) -> u16 {
        u16::from_ne_bytes(src.try_into().expect("slice length checked"))
    }

    fn read_u32_ne(src: &[u8]) -> u32 {
        u32::from_ne_bytes(src.try_into().expect("slice length checked"))
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::telemetry::buffer;
        use crate::telemetry::recorder::SharedState;
        use serde::Deserialize;
        use std::collections::HashMap;

        #[derive(Debug, Deserialize)]
        #[serde(tag = "event")]
        enum DecodedEvent {
            SocketAcceptQueueEvent(DecodedSocketAcceptQueueEvent),
            #[serde(other)]
            Other,
        }

        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct DecodedSocketAcceptQueueEvent {
            timestamp_ns: u64,
            address_family: u8,
            protocol: u8,
            local_addr: String,
            local_port: u16,
            socket_inode: u64,
            pending_connections: u64,
            backlog_limit: u64,
        }

        fn decode_socket_accept_queue_events(bytes: &[u8]) -> Vec<DecodedSocketAcceptQueueEvent> {
            let mut decoder = dial9_trace_format::decoder::Decoder::new(bytes)
                .expect("encoded socket accept queue batch should have a valid trace header");
            let mut events = Vec::new();
            decoder
                .for_each_event(|raw| {
                    match raw
                        .deserialize()
                        .expect("encoded socket accept queue event should deserialize")
                    {
                        DecodedEvent::SocketAcceptQueueEvent(event) => events.push(event),
                        DecodedEvent::Other => {}
                    }
                })
                .expect("encoded socket accept queue batch should decode");
            events
        }

        #[test]
        fn parse_socket_inode_extracts_proc_fd_socket_target() {
            assert_eq!(parse_socket_inode(Path::new("socket:[12345]")), Some(12345));
            assert_eq!(parse_socket_inode(Path::new("/tmp/file")), None);
        }

        #[test]
        fn read_socket_accept_queues_returns_current_process_listeners() {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("test listener should bind to loopback");
            let addr = listener
                .local_addr()
                .expect("test listener should have a local address");
            let pending_client = std::net::TcpStream::connect(addr)
                .expect("client connection should enter the listener accept queue");

            let snapshots = read_socket_accept_queues()
                .expect("sock_diag should read current process TCP listeners");

            drop(pending_client);
            drop(listener);

            assert!(
                snapshots.iter().any(|snapshot| {
                    snapshot.local_port == addr.port()
                        && snapshot.pending_connections >= 1
                        && snapshot.backlog_limit >= snapshot.pending_connections
                }),
                "expected socket accept queue snapshot for listener {addr}; got {snapshots:?}"
            );
        }

        #[test]
        fn source_emits_socket_accept_queue_events() {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("test listener should bind to loopback");
            let addr = listener
                .local_addr()
                .expect("test listener should have a local address");
            let pending_client = std::net::TcpStream::connect(addr)
                .expect("client connection should enter the listener accept queue");

            let shared = SharedState::new(0, None);
            let thread_roles = HashMap::new();
            let ctx = FlushContext {
                collector: &shared.collector,
                drain_epoch: &shared.drain_epoch,
                thread_roles: &thread_roles,
            };
            let mut source = SocketAcceptQueuesSource::new(SocketAcceptQueuesConfig::default());

            source.flush(&ctx);
            buffer::drain_to_collector(&shared.collector);

            drop(pending_client);
            drop(listener);

            let batch = shared.collector.next().expect("source should emit a batch");
            let events = decode_socket_accept_queue_events(batch.encoded_bytes());

            assert!(
                events.iter().any(|event| {
                    event.local_port == addr.port()
                        && event.pending_connections >= 1
                        && event.backlog_limit >= event.pending_connections
                }),
                "expected socket accept queue event for listener {addr}; got {events:?}"
            );
        }

        #[test]
        fn source_respects_sample_interval() {
            let shared = SharedState::new(0, None);
            let thread_roles = HashMap::new();
            let ctx = FlushContext {
                collector: &shared.collector,
                drain_epoch: &shared.drain_epoch,
                thread_roles: &thread_roles,
            };
            let config = SocketAcceptQueuesConfig::builder()
                .sample_interval(Duration::from_secs(60))
                .build();
            let mut source = SocketAcceptQueuesSource::new(config);

            source.flush(&ctx);
            let first_sample = source.last_sample;
            source.flush(&ctx);

            assert_eq!(source.last_sample, first_sample);
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
