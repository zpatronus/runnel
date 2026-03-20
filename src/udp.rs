use anyhow::Result;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::UdpSocket;

pub struct Client {
    socket: UdpSocket,
    remote: SocketAddr,
}

impl Client {
    pub async fn new(bind_addr: &str, remote_addr: &str) -> Result<Self> {
        Ok(Self {
            socket: UdpSocket::bind(bind_addr).await?,
            remote: remote_addr.parse()?,
        })
    }

    pub async fn send(&self, data: &[u8]) -> Result<()> {
        self.socket.send_to(data, self.remote).await?;
        Ok(())
    }

    pub async fn recv(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; 65535];
        let (len, _) = self.socket.recv_from(&mut buf).await?;
        buf.truncate(len);
        Ok(buf)
    }
}

pub struct Server {
    socket: Arc<UdpSocket>,
}

pub struct Reply {
    socket: Arc<UdpSocket>,
    to: SocketAddr,
}

impl Server {
    pub async fn new(bind_addr: &str) -> Result<Self> {
        Ok(Self {
            socket: Arc::new(UdpSocket::bind(bind_addr).await?),
        })
    }

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
    pub async fn send(&self, data: &[u8]) -> Result<()> {
        self.socket.send_to(data, self.to).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
