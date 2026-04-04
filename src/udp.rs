//! A UDP client and server implementation using Tokio.

use anyhow::Result;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::UdpSocket;

/// UDP client for sending data to a remote address and receiving responses.
#[derive(Clone)]
pub struct Client {
    socket: Arc<UdpSocket>,
    remote: SocketAddr,
}

impl Client {
    /// Creates a new UDP client bound to `bind_addr` and configured to send to `remote_addr`.
    pub async fn new(bind_addr: &str, remote_addr: &str) -> Result<Self> {
        Ok(Self {
            socket: Arc::new(UdpSocket::bind(bind_addr).await?),
            remote: remote_addr.parse()?,
        })
    }

    /// Sends data to the remote address.
    pub async fn send(&self, data: &[u8]) -> Result<()> {
        self.socket.send_to(data, self.remote).await?;
        Ok(())
    }

    /// Receives data from the socket.
    pub async fn recv(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; 65535];
        let (len, _) = self.socket.recv_from(&mut buf).await?;
        buf.truncate(len);
        Ok(buf)
    }
}

/// UDP server that listens for incoming packets and can send responses.
pub struct Server {
    socket: Arc<UdpSocket>,
}

/// Reply object for sending responses back to the sender of a received packet.
pub struct Reply {
    socket: Arc<UdpSocket>,
    to: SocketAddr,
}

impl Server {
    /// Creates a new UDP server bound to `bind_addr`.
    pub async fn new(bind_addr: &str) -> Result<Self> {
        Ok(Self {
            socket: Arc::new(UdpSocket::bind(bind_addr).await?),
        })
    }

    /// Receives a packet and returns the data along with a `Reply` for sending a response.
    pub async fn recv(&self) -> Result<(Vec<u8>, Reply)> {
        let mut buf = vec![0u8; 65535];
        let (len, from) = self.socket.recv_from(&mut buf).await?;
        buf.truncate(len);

        Ok((
            buf,
            Reply {
                socket: self.socket.clone(),
                to: from,
            },
        ))
    }
}

impl Reply {
    /// Sends a response back to the original sender. Consumes the reply.
    pub async fn send(self, data: &[u8]) -> Result<()> {
        self.socket.send_to(data, self.to).await?;
        Ok(())
    }
}

#[cfg(test)]
mod udp_tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_client_server_hello_world() -> Result<()> {
        let server_addr = "127.0.0.1:12345";
        let server = Server::new(server_addr).await?;
        let server_handle = tokio::spawn(async move {
            let (data, reply) = server.recv().await.unwrap();
            assert_eq!(data, b"hello");
            reply.send(b"world").await.unwrap();
        });
        let client = Client::new("0.0.0.0:0", server_addr).await?;
        client.send(b"hello").await?;

        let response = timeout(Duration::from_secs(5), client.recv()).await??;

        assert_eq!(response, b"world");

        timeout(Duration::from_secs(5), server_handle).await??;

        Ok(())
    }
}
