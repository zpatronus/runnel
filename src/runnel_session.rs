use crate::dns_endec::{DnsRequest, DnsResponse};
use crate::kcp_session::KcpSession;
use crate::udp::{Client, Server};
use anyhow::{Result, bail};
use kcp::get_conv;
use std::time::{Duration, Instant};
use tokio::{self, sync::broadcast, time};

const NOOP_MESSAGE: &[u8] = b"ff2f56ce-fa05-4c77-ba07-c17776d03db2";
const NOOP_INTERVAL: Duration = Duration::from_millis(10);

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
                            if let Ok(data) = result {
                                if let Ok(decoded) = response_decoder.decode_packet(&data) {
                                    let _ = kcp_session.send(NOOP_MESSAGE);
                                    let _ = kcp_session.send(NOOP_MESSAGE);
                                    let decoded_conv = get_conv(&decoded);
                                    if decoded_conv == conv {
                                        let _ = kcp_session.input_packet(&decoded);
                                    }
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
                        let _ = kcp_session.send(NOOP_MESSAGE);
                        last_send_time = Instant::now();
                        did_work = true;
                    }
                    if !did_work {
                        tokio::select! {
                            _ = time::sleep(Duration::from_millis(1)) => {}
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

    pub fn send_noop(&mut self) -> Result<()> {
        self.kcp_session.send(NOOP_MESSAGE)?;
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
    request_decoder: DnsRequest,
    response_encoder: DnsResponse,
    udp_server: Server,
}
