use crate::dns_endec::{DnsRequest, DnsResponse};
use crate::kcp_session::KcpSession;
use crate::udp::{Client, Server};
use anyhow::{Result, bail};
use kcp::{KCP_OVERHEAD, get_conv};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::{self, sync::broadcast, time};

const NOOP_MESSAGE: &[u8] = b"ff2f56ce-fa05-4c77-ba07-c17776d03db2";
const NOOP_INTERVAL: Duration = Duration::from_millis(10);
const POLL_INTERVAL: Duration = Duration::from_millis(1);
const DEFAULT_SERVER_TIMEOUT: Duration = Duration::from_secs(60);

fn construct_noop_message() -> Vec<u8> {
    let mut msg = NOOP_MESSAGE.to_vec();
    let random_bytes: [u8; 32] = rand::random();
    msg.extend_from_slice(&random_bytes);
    msg
}

fn is_noop_message(msg: &[u8]) -> bool {
    msg.starts_with(NOOP_MESSAGE)
}

pub struct RunnelClient {
    kcp_session: KcpSession,
    _shutdown: broadcast::Sender<()>,
}

impl RunnelClient {
    pub async fn new(dns_servers: Vec<String>, domain_suffix: &str) -> Result<Self> {
        if dns_servers.is_empty() {
            bail!("At least one DNS server must be provided");
        }
        let mut udp_clients = Vec::new();
        for server in &dns_servers {
            udp_clients.push(Client::new("0.0.0.0:0", server).await?);
        }
        let request_encoder = DnsRequest::new(domain_suffix)?;
        let mtu = request_encoder.max_data_len();
        let conv = rand::random::<u32>();
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
                                    let _ = kcp_session.send(&construct_noop_message());
                                    let _ = kcp_session.send(&construct_noop_message());
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

    pub fn send(&mut self, data: &[u8]) -> Result<()> {
        self.kcp_session.send(data)?;
        Ok(())
    }

    fn send_noop(&mut self) -> Result<()> {
        self.kcp_session.send(&construct_noop_message())?;
        Ok(())
    }

    pub fn recv(&mut self) -> Option<Vec<u8>> {
        loop {
            if let Some(msg) = self.kcp_session.recv() {
                let _ = self.send_noop();
                let _ = self.send_noop();
                if !is_noop_message(&msg) {
                    return Some(msg);
                }
            } else {
                return None;
            }
        }
    }
}

pub struct RunnelServer {
    kcp_session: Arc<Mutex<Option<(u32, KcpSession)>>>,
    _shutdown: broadcast::Sender<()>,
}

impl RunnelServer {
    pub async fn new(bind_addr: &str, domain_suffix: &str) -> Result<Self> {
        let udp_server = Server::new(bind_addr).await?;
        let request_decoder = DnsRequest::new(domain_suffix)?;
        let response_encoder = DnsResponse::new();
        let mtu = response_encoder.max_data_len();

        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let kcp_session: Arc<Mutex<Option<(u32, KcpSession)>>> = Arc::new(Mutex::new(None));

        {
            let kcp_session = kcp_session.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        result = udp_server.recv() => {
                            if let Ok((data, reply)) = result && let Ok(decoded) = request_decoder.decode_packet(&data) {
                                    let conv = get_conv(&decoded);
                                    let packet = {
                                        let mut state = kcp_session.lock().unwrap();
                                        if state.is_none() {
                                            *state = Some((conv, KcpSession::new(conv, mtu)));
                                        }
                                        let (stored_conv, kcp) = state.as_ref().unwrap();
                                        if *stored_conv != conv {
                                            continue;
                                        }
                                        let _ = kcp.input_packet(&decoded);
                                        kcp.poll_output_packet()
                                    };
                                    if let Some(packet) = packet {
                                        if let Ok(encoded) = response_encoder.encode_packet(&data, &packet) {
                                            let _ = reply.send(&encoded).await;
                                        }
                                    } else {
                                        // this is fine, since client checks conv
                                        if let Ok(encoded) = response_encoder.encode_packet(&data, b"") {
                                            let _ = reply.send(&encoded).await;
                                        }
                                    }
                            }
                        }
                        _ = time::sleep(DEFAULT_SERVER_TIMEOUT) => {
                            *kcp_session.lock().unwrap() = None;
                        }
                        _ = shutdown_rx.recv() => break,
                    }
                }
            });
        }

        Ok(Self {
            kcp_session,
            _shutdown: shutdown_tx,
        })
    }

    pub fn send(&mut self, data: &[u8]) -> Result<()> {
        let state = self.kcp_session.lock().unwrap();
        match state.as_ref() {
            Some((_, kcp)) => kcp.send(data)?,
            None => bail!("No active session"),
        }
        Ok(())
    }

    pub fn recv(&mut self) -> Option<Vec<u8>> {
        let state = self.kcp_session.lock().unwrap();
        state.as_ref()?.1.recv()
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
        let mut server = RunnelServer::new(&server_addr, "test.com").await?;
        let mut client = RunnelClient::new(vec![server_addr], "test.com").await?;

        client.send(b"hello from client")?;

        for _ in 0..200 {
            time::sleep(Duration::from_millis(5)).await;
            if let Some(msg) = server.recv() {
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
        let mut server = RunnelServer::new(&server_addr, "test.com").await?;
        let mut client = RunnelClient::new(vec![server_addr], "test.com").await?;

        client.send(b"hello from client")?;

        for _ in 0..200 {
            time::sleep(Duration::from_millis(5)).await;
            if let Some(msg) = server.recv() {
                assert_eq!(msg, b"hello from client");
                server.send(b"hello from server")?;

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
        let mut server = RunnelServer::new(&server_addr, "test.com").await?;
        let mut client = RunnelClient::new(vec![server_addr.clone()], "test.com").await?;

        let long_msg: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();

        client.send(b"init")?;
        for _ in 0..500 {
            time::sleep(Duration::from_millis(5)).await;
            if server.recv().is_some() {
                println!("Received init message, proceeding with long message test");
                break;
            }
        }

        client.send(&long_msg)?;
        server.send(&long_msg)?;

        let server_handle = tokio::spawn(async move {
            for _ in 0..2000 {
                time::sleep(Duration::from_millis(5)).await;
                if let Some(msg) = server.recv() {
                    return Ok::<Option<Vec<u8>>, anyhow::Error>(Some(msg));
                }
            }
            Ok(None)
        });

        let client_handle = tokio::spawn(async move {
            for _ in 0..2000 {
                time::sleep(Duration::from_millis(5)).await;
                if let Some(msg) = client.recv() {
                    return Ok::<Option<Vec<u8>>, anyhow::Error>(Some(msg));
                }
            }
            Ok(None)
        });

        let server_msg = server_handle.await??;
        let client_msg = client_handle.await??;

        let server_msg = server_msg.context("server never received message")?;
        let client_msg = client_msg.context("client never received message")?;

        assert_eq!(server_msg, long_msg);
        assert_eq!(client_msg, long_msg);
        Ok(())
    }
}
