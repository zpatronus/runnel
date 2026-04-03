use crate::dns_endec::{DnsRequest, DnsResponse};
use crate::kcp_session::KcpSession;
use crate::udp::{Client, Server};
use anyhow::{Ok, Result, bail};

const NOOP_MESSAGE: &[u8] = b"ff2f56ce-fa05-4c77-ba07-c17776d03db2";

fn is_noop_message(msg: &[u8]) -> bool {
    msg.starts_with(NOOP_MESSAGE)
}

pub struct RunnelClient {
    dns_servers: Vec<String>,
    udp_clients: Vec<Client>,
    request_encoder: DnsRequest,
    response_decoder: DnsResponse,
    kcp_session: KcpSession,
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
        let conv = rand::random::<u32>();
        let request_encoder = DnsRequest::new(domain_suffix)?;
        let mtu = request_encoder.max_data_len();
        todo!("add daemon loop");
        Ok(Self {
            dns_servers,
            udp_clients,
            request_encoder: DnsRequest::new(domain_suffix)?,
            response_decoder: DnsResponse::new(),
            kcp_session: KcpSession::new(conv, mtu),
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
