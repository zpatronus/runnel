//! Session layer over KCP for DNS tunneling.
use crate::dns_endec::{DnsRequest, DnsResponse};
use crate::kcp_session::KcpSession;
use crate::udp::{Client, Server};
use anyhow::{Result, bail};
use kcp::{KCP_OVERHEAD, get_conv};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::{self, sync::broadcast, time};

/// Prefix for NOOP messages used to keep the KCP session alive.
const NOOP_MESSAGE: &[u8] = b"ff2f56ce-fa05-4c77-ba07-c17776d03db2";
/// Interval for sending NOOP messages when idle.
const NOOP_INTERVAL: Duration = Duration::from_millis(10);
/// Interval for polling output packets.
const POLL_INTERVAL: Duration = Duration::from_millis(1);
/// Default timeout for cleaning up inactive server sessions.
const DEFAULT_SERVER_TIMEOUT: Duration = Duration::from_secs(10);

/// Creates a NOOP message with the fixed prefix and 32 random bytes.
fn construct_noop_message() -> Vec<u8> {
    let mut msg = NOOP_MESSAGE.to_vec();
    let random_bytes: [u8; 32] = rand::random();
    msg.extend_from_slice(&random_bytes);
    msg
}

/// Checks if a message is a NOOP message.
fn is_noop_message(msg: &[u8]) -> bool {
    msg.starts_with(NOOP_MESSAGE)
}

/// Client that communicates with a `RunnelServer` using KCP over DNS.
pub struct RunnelClient {
    kcp_session: KcpSession,
    _shutdown: broadcast::Sender<()>,
}

impl RunnelClient {
    /// Creates a new client connected to the specified DNS servers.
    ///
    /// Uses a random conversation ID. Background tasks handle packet sending/receiving.
    pub async fn new(dns_servers: Vec<String>, domain_suffix: &str) -> Result<Self> {
        Self::with_conv(dns_servers, domain_suffix, rand::random::<u32>()).await
    }

    /// Creates a new client with a specified conversation ID.
    ///
    /// Useful for testing with deterministic conversation IDs.
    pub async fn with_conv(
        dns_servers: Vec<String>,
        domain_suffix: &str,
        conv: u32,
    ) -> Result<Self> {
        if dns_servers.is_empty() {
            bail!("At least one DNS server must be provided");
        }
        let mut udp_clients = Vec::new();
        for server in &dns_servers {
            udp_clients.push(Client::new("0.0.0.0:0", server).await?);
        }
        let request_encoder = DnsRequest::new(domain_suffix)?;
        let mtu = request_encoder.max_data_len();
        let kcp_session = KcpSession::new(conv, mtu);

        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        // receive responses from DNS servers
        for udp_client in &udp_clients {
            let udp_client = udp_client.clone();
            let kcp_session = kcp_session.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            tokio::spawn(async move {
                let response_decoder = DnsResponse::new();
                loop {
                    tokio::select! {
                        result = udp_client.recv() => {
                            if let Ok(data) = result && let Ok(decoded) = response_decoder.decode_packet(&data) {
                                    if decoded.len() < KCP_OVERHEAD {
                                        continue;
                                    }
                                    let decoded_conv = get_conv(&decoded);
                                    if decoded_conv == conv {
                                        let _ = kcp_session.input_packet(&decoded);
                                    }
                            }
                        }
                        _ = shutdown_rx.recv() => break,
                    }
                }
            });
        }

        // send requests to DNS servers
        {
            let kcp_session = kcp_session.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            tokio::spawn(async move {
                let mut to_send_udp_idx_1 = 0;
                let mut to_send_udp_idx_2 = udp_clients.len() - 1;
                let mut last_send_time = Instant::now();
                loop {
                    let mut did_work = false;
                    to_send_udp_idx_1 = (to_send_udp_idx_1 + 1) % udp_clients.len();
                    to_send_udp_idx_2 = (to_send_udp_idx_2 + 1) % udp_clients.len();
                    if let Some(packet) = kcp_session.poll_output_packet() {
                        if let Ok(encoded) = request_encoder.encode_packet(&packet) {
                            let _ = udp_clients[to_send_udp_idx_1].send(&encoded).await;
                            let _ = udp_clients[to_send_udp_idx_2].send(&encoded).await;
                        } else {
                            println!("Failed to encode packet");
                        }
                        did_work = true;
                        last_send_time = Instant::now();
                    }
                    if !did_work && last_send_time.elapsed() >= NOOP_INTERVAL {
                        let _ = kcp_session.send(&construct_noop_message());
                        last_send_time = Instant::now();
                        did_work = true;
                    }
                    if !did_work {
                        tokio::select! {
                            _ = time::sleep(POLL_INTERVAL) => {}
                            _ = shutdown_rx.recv() => break,
                        }
                    }
                }
            });
        }

        Ok(Self {
            kcp_session,
            _shutdown: shutdown_tx,
        })
    }

    /// Sends data through the KCP session.
    pub fn send(&mut self, data: &[u8]) -> Result<()> {
        self.kcp_session.send(data)?;
        Ok(())
    }

    /// Returns the conversation ID.
    pub fn get_conv(&self) -> u32 {
        self.kcp_session.conv()
    }

    /// Sends a NOOP message to keep the session alive.
    fn send_noop(&mut self) -> Result<()> {
        self.kcp_session.send(&construct_noop_message())?;
        Ok(())
    }

    /// Receives data from the KCP session, filtering out NOOP messages.
    pub fn recv(&mut self) -> Option<Vec<u8>> {
        loop {
            if let Some(msg) = self.kcp_session.recv() {
                if !is_noop_message(&msg) {
                    let _ = self.send_noop();
                    let _ = self.send_noop();
                    return Some(msg);
                }
            } else {
                return None;
            }
        }
    }
}

/// Server-side session managing a single client's KCP connection.
struct ServerSession {
    kcp: KcpSession,
    last_active: Instant,
}

/// Server that handles multiple clients communicating using KCP over DNS.
///
/// Manages multiple KCP sessions identified by conversation IDs.
pub struct RunnelServer {
    sessions: Arc<Mutex<HashMap<u32, ServerSession>>>,
    _shutdown: broadcast::Sender<()>,
}

impl RunnelServer {
    /// Creates a new server bound to the specified address with default session timeout.
    pub async fn new(bind_addr: &str, domain_suffix: &str) -> Result<Self> {
        Self::with_timeout(bind_addr, domain_suffix, DEFAULT_SERVER_TIMEOUT).await
    }

    /// Creates a new server with a specified session timeout.
    ///
    /// Inactive sessions are cleaned up after the timeout duration.
    pub async fn with_timeout(
        bind_addr: &str,
        domain_suffix: &str,
        timeout: Duration,
    ) -> Result<Self> {
        let udp_server = Server::new(bind_addr).await?;
        let request_decoder = DnsRequest::new(domain_suffix)?;
        let response_encoder = DnsResponse::new();
        let mtu = response_encoder.max_data_len();

        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let sessions: Arc<Mutex<HashMap<u32, ServerSession>>> =
            Arc::new(Mutex::new(HashMap::new()));

        {
            let sessions = sessions.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        result = udp_server.recv() => {
                            if let Ok((data, reply)) = result && let Ok(decoded) = request_decoder.decode_packet(&data) {
                                let conv = get_conv(&decoded);
                                let packet = {
                                    let mut state = sessions.lock().unwrap();
                                    let session = state.entry(conv).or_insert_with(|| {
                                        ServerSession {
                                            kcp: KcpSession::new(conv, mtu),
                                            last_active: Instant::now(),
                                        }
                                    });
                                    session.last_active = Instant::now();
                                    let _ = session.kcp.input_packet(&decoded);
                                    session.kcp.poll_output_packet()
                                };
                                if let Some(packet) = packet {
                                    if let Ok(encoded) = response_encoder.encode_packet(&data, &packet) {
                                        let _ = reply.send(&encoded).await;
                                    }
                                } else {
                                    // this is fine, since client checks packet size
                                    if let Ok(encoded) = response_encoder.encode_packet(&data, b"") {
                                        let _ = reply.send(&encoded).await;
                                    }
                                }
                            }
                        }
                        _ = time::sleep(timeout) => {
                            let mut state = sessions.lock().unwrap();
                            state.retain(|_, session| session.last_active.elapsed() < timeout);
                        }
                        _ = shutdown_rx.recv() => break,
                    }
                }
            });
        }

        Ok(Self {
            sessions,
            _shutdown: shutdown_tx,
        })
    }

    /// Sends data to the client with the specified conversation ID.
    ///
    /// Returns an error if no active session exists for the conversation ID.
    pub fn send(&self, conv: u32, data: &[u8]) -> Result<()> {
        let state = self.sessions.lock().unwrap();
        match state.get(&conv) {
            Some(session) => session.kcp.send(data)?,
            None => bail!("No active session for conv {}", conv),
        }
        Ok(())
    }

    /// Receives data from the client with the specified conversation ID, filtering out NOOP messages.
    ///
    /// Returns `None` if no active session exists or no data is available.
    pub fn recv(&self, conv: u32) -> Option<Vec<u8>> {
        let state = self.sessions.lock().unwrap();
        let session = state.get(&conv)?;
        loop {
            if let Some(msg) = session.kcp.recv() {
                if !is_noop_message(&msg) {
                    return Some(msg);
                }
            } else {
                return None;
            }
        }
    }

    /// Returns a list of active conversation IDs.
    pub fn active_convs(&self) -> Vec<u32> {
        let state = self.sessions.lock().unwrap();
        state.keys().copied().collect()
    }
}

#[cfg(test)]
mod runnel_session_tests {
    use super::*;
    use anyhow::Context;
    use std::net::UdpSocket;

    fn find_available_port() -> u16 {
        UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[tokio::test]
    async fn test_client_to_server_short_message() -> Result<()> {
        let port = find_available_port();
        let server_addr = format!("127.0.0.1:{}", port);
        let server = RunnelServer::new(&server_addr, "test.com").await?;
        let mut client = RunnelClient::new(vec![server_addr], "test.com").await?;
        let conv = client.get_conv();

        client.send(b"hello from client")?;

        for _ in 0..200 {
            time::sleep(Duration::from_millis(5)).await;
            if let Some(msg) = server.recv(conv) {
                assert_eq!(msg, b"hello from client");
                return Ok(());
            }
        }
        bail!("server never received client message");
    }

    #[tokio::test]
    async fn test_exchange_short_message() -> Result<()> {
        let port = find_available_port();
        let server_addr = format!("127.0.0.1:{}", port);
        let server = RunnelServer::new(&server_addr, "test.com").await?;
        let mut client = RunnelClient::new(vec![server_addr], "test.com").await?;
        let conv = client.get_conv();

        client.send(b"hello from client")?;

        for _ in 0..200 {
            time::sleep(Duration::from_millis(5)).await;
            if let Some(msg) = server.recv(conv) {
                assert_eq!(msg, b"hello from client");
                server.send(conv, b"hello from server")?;

                for _ in 0..200 {
                    time::sleep(Duration::from_millis(5)).await;
                    if let Some(msg) = client.recv() {
                        assert_eq!(msg, b"hello from server");
                        return Ok(());
                    }
                }
                bail!("client never received server message");
            }
        }
        bail!("server never received client message");
    }

    #[tokio::test]
    async fn test_exchange_long_message() -> Result<()> {
        let port = find_available_port();
        let server_addr = format!("127.0.0.1:{}", port);
        let server = RunnelServer::new(&server_addr, "test.com").await?;
        let mut client = RunnelClient::new(vec![server_addr.clone()], "test.com").await?;
        let conv = client.get_conv();

        let long_msg: Vec<u8> = (0..500000).map(|i| (i % 256) as u8).collect();

        client.send(b"init")?;
        for _ in 0..500 {
            time::sleep(Duration::from_millis(5)).await;
            if let Some(msg) = server.recv(conv) {
                if msg == b"init" {
                    break;
                }
            }
        }

        client.send(&long_msg)?;
        server.send(conv, &long_msg)?;

        let mut server_msg: Option<Vec<u8>> = None;
        let mut client_msg: Option<Vec<u8>> = None;
        for _ in 0..2000 {
            time::sleep(Duration::from_millis(5)).await;
            if server_msg.is_none() {
                server_msg = server.recv(conv);
            }
            if client_msg.is_none() {
                client_msg = client.recv();
            }
            if server_msg.is_some() && client_msg.is_some() {
                break;
            }
        }

        let server_msg = server_msg.context("server never received message")?;
        let client_msg = client_msg.context("client never received message")?;

        assert_eq!(server_msg, long_msg);
        assert_eq!(client_msg, long_msg);
        Ok(())
    }

    #[tokio::test]
    async fn test_benchmark() -> Result<()> {
        let port = find_available_port();
        let server_addr = format!("127.0.0.1:{}", port);
        let server = RunnelServer::new(&server_addr, "test.com").await?;
        let mut client = RunnelClient::new(vec![server_addr], "test.com").await?;
        let conv = client.get_conv();

        client.send(b"init")?;
        for _ in 0..500 {
            time::sleep(Duration::from_millis(5)).await;
            if let Some(msg) = server.recv(conv) {
                if msg == b"init" {
                    break;
                }
            }
        }

        let msg_size: usize = 100000;
        let bench_duration = Duration::from_secs(15);
        let data: Vec<u8> = vec![0u8; msg_size];

        let mut server_recv_count: u64 = 0;
        let mut client_recv_count: u64 = 0;

        client.send(&data)?;
        server.send(conv, &data)?;

        let start = Instant::now();
        while start.elapsed() < bench_duration {
            time::sleep(Duration::from_millis(1)).await;

            if server.recv(conv).is_some() {
                server_recv_count += 1;
                let _ = server.send(conv, &data);
            }

            if client.recv().is_some() {
                client_recv_count += 1;
                let _ = client.send(&data);
            }
        }
        let elapsed = start.elapsed().as_secs_f64();

        let c2s_bytes = server_recv_count as f64 * msg_size as f64;
        let s2c_bytes = client_recv_count as f64 * msg_size as f64;
        let c2s_speed = c2s_bytes / elapsed / 1_000_000.0;
        let s2c_speed = s2c_bytes / elapsed / 1_000_000.0;

        println!("Client -> Server: {:.2} MB/s", c2s_speed);
        println!("Server -> Client: {:.2} MB/s", s2c_speed);

        Ok(())
    }

    #[tokio::test]
    async fn test_server_timeout() -> Result<()> {
        let port = find_available_port();
        let server_addr = format!("127.0.0.1:{}", port);
        let server =
            RunnelServer::with_timeout(&server_addr, "test.com", Duration::from_secs(3)).await?;

        // t=0: client1 connects and exchanges with server
        let conv1 = 12345u32;
        let mut client1 =
            RunnelClient::with_conv(vec![server_addr.clone()], "test.com", conv1).await?;
        client1.send(b"hello")?;
        for _ in 0..200 {
            time::sleep(Duration::from_millis(5)).await;
            if let Some(msg) = server.recv(conv1) {
                let reply: Vec<u8> = msg.iter().map(|b| b.to_ascii_uppercase()).collect();
                server.send(conv1, &reply)?;
                break;
            }
        }
        for _ in 0..200 {
            time::sleep(Duration::from_millis(5)).await;
            if let Some(msg) = client1.recv() {
                assert_eq!(msg, b"HELLO");
                break;
            }
        }

        // drop client1 and wait for server timeout
        drop(client1);
        time::sleep(Duration::from_secs(5)).await;

        // t=5: client2 reuses the same conv
        let mut client2 = RunnelClient::with_conv(vec![server_addr], "test.com", conv1).await?;
        client2.send(b"world")?;
        for _ in 0..200 {
            time::sleep(Duration::from_millis(5)).await;
            if let Some(msg) = server.recv(conv1) {
                let reply: Vec<u8> = msg.iter().map(|b| b.to_ascii_uppercase()).collect();
                server.send(conv1, &reply)?;
                break;
            }
        }
        for _ in 0..200 {
            time::sleep(Duration::from_millis(5)).await;
            if let Some(msg) = client2.recv() {
                assert_eq!(msg, b"WORLD");
                return Ok(());
            }
        }
        bail!("client2 never received response");
    }

    #[tokio::test]
    async fn test_three_clients_500_bytes() -> Result<()> {
        let port = find_available_port();
        let server_addr = format!("127.0.0.1:{}", port);
        let server = RunnelServer::new(&server_addr, "test.com").await?;
        let mut client1 = RunnelClient::new(vec![server_addr.clone()], "test.com").await?;
        let mut client2 = RunnelClient::new(vec![server_addr.clone()], "test.com").await?;
        let mut client3 = RunnelClient::new(vec![server_addr], "test.com").await?;
        let conv1 = client1.get_conv();
        let conv2 = client2.get_conv();
        let conv3 = client3.get_conv();

        let msg: Vec<u8> = (0..500).map(|i| (i % 256) as u8).collect();

        client1.send(b"init")?;
        client2.send(b"init")?;
        client3.send(b"init")?;
        for _ in 0..500 {
            time::sleep(Duration::from_millis(5)).await;
            if server.recv(conv1).is_some()
                && server.recv(conv2).is_some()
                && server.recv(conv3).is_some()
            {
                break;
            }
        }

        client1.send(&msg)?;
        client2.send(&msg)?;
        client3.send(&msg)?;
        server.send(conv1, &msg)?;
        server.send(conv2, &msg)?;
        server.send(conv3, &msg)?;

        let mut s_got1 = false;
        let mut s_got2 = false;
        let mut s_got3 = false;
        let mut c_got1 = false;
        let mut c_got2 = false;
        let mut c_got3 = false;
        for _ in 0..2000 {
            time::sleep(Duration::from_millis(5)).await;
            if !s_got1 {
                if let Some(data) = server.recv(conv1) {
                    assert_eq!(data, msg);
                    s_got1 = true;
                }
            }
            if !s_got2 {
                if let Some(data) = server.recv(conv2) {
                    assert_eq!(data, msg);
                    s_got2 = true;
                }
            }
            if !s_got3 {
                if let Some(data) = server.recv(conv3) {
                    assert_eq!(data, msg);
                    s_got3 = true;
                }
            }
            if !c_got1 {
                if let Some(data) = client1.recv() {
                    assert_eq!(data, msg);
                    c_got1 = true;
                }
            }
            if !c_got2 {
                if let Some(data) = client2.recv() {
                    assert_eq!(data, msg);
                    c_got2 = true;
                }
            }
            if !c_got3 {
                if let Some(data) = client3.recv() {
                    assert_eq!(data, msg);
                    c_got3 = true;
                }
            }
            if s_got1 && s_got2 && s_got3 && c_got1 && c_got2 && c_got3 {
                return Ok(());
            }
        }
        bail!("not all server/client exchanges completed");
    }
}
