use anyhow::Result;
use kcp::Kcp;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

#[derive(Clone)]
struct OutputPacketBuf(Arc<Mutex<Vec<Vec<u8>>>>);

impl Write for OutputPacketBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut output_buf = self.0.lock().unwrap();
        output_buf.push(buf.to_vec());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct KcpSession {
    kcp_ptr: Arc<Mutex<Kcp<OutputPacketBuf>>>,
    output_packet_buf_ptr: OutputPacketBuf,
    recv_buf: Arc<Mutex<Vec<u8>>>,
    _is_closed: mpsc::Sender<()>,
}

impl KcpSession {
    pub fn new(conv: u32, mtu: usize) -> Self {
        let output = OutputPacketBuf(Arc::new(Mutex::new(Vec::<Vec<u8>>::new())));
        let mut kcp = Kcp::new(conv, output.clone());
        kcp.set_nodelay(true, 20, 2, true);
        kcp.set_wndsize(15000, 15000);
        kcp.set_mtu(mtu).unwrap();
        let kcp = Arc::new(Mutex::new(kcp));
        let (tx, mut rx) = mpsc::channel::<()>(1);

        let kcp_clone = kcp.clone();

        tokio::spawn(async move {
            let tick_interval = Duration::from_millis(20);
            loop {
                tokio::select! {
                                    _ = tokio::time::sleep(tick_interval) => {
                                        let mut kcp = kcp_clone.lock().unwrap();
                                        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u32;
                                        let _ = kcp.update(now);
                                    }
                                    _ = rx.recv() => {
                                        break;
                                    }
                }
            }
        });

        Self {
            kcp_ptr: kcp,
            output_packet_buf_ptr: output,
            recv_buf: Arc::new(Mutex::new(Vec::new())),
            _is_closed: tx,
        }
    }

    fn kcp(&self) -> std::sync::MutexGuard<'_, Kcp<OutputPacketBuf>> {
        self.kcp_ptr.lock().unwrap()
    }

    fn output_packet_buf(&self) -> std::sync::MutexGuard<'_, Vec<Vec<u8>>> {
        self.output_packet_buf_ptr.0.lock().unwrap()
    }

    pub fn send(&self, data: &[u8]) -> Result<()> {
        let max_data = (self.kcp().mss() * 127).saturating_sub(1).max(1);
        let chunks: Vec<&[u8]> = data.chunks(max_data).collect();
        for (i, chunk) in chunks.iter().enumerate() {
            let has_more = i < chunks.len() - 1;
            let mut packet = vec![if has_more { 1u8 } else { 0u8 }];
            packet.extend_from_slice(chunk);
            self.kcp()
                .send(&packet)
                .map_err(|e| anyhow::anyhow!("Failed to send data: {:?}", e))?;
        }
        Ok(())
    }

    pub fn recv(&self) -> Option<Vec<u8>> {
        let mut recv_buf = self.recv_buf.lock().unwrap();
        loop {
            let size = match self.kcp().peeksize().ok() {
                Some(s) => s,
                None => break,
            };
            let mut buf = vec![0u8; size];
            self.kcp().recv(&mut buf).ok()?;
            let has_more = buf[0] != 0;
            recv_buf.extend_from_slice(&buf[1..]);
            if !has_more {
                let result = recv_buf.clone();
                recv_buf.clear();
                return Some(result);
            }
        }
        None
    }

    pub fn poll_output_packet(&self) -> Option<Vec<u8>> {
        let mut output_buf = self.output_packet_buf();
        if output_buf.is_empty() {
            None
        } else {
            Some(output_buf.remove(0))
        }
    }

    pub fn input_packet(&self, packet: &[u8]) -> Result<()> {
        self.kcp()
            .input(packet)
            .map_err(|e| anyhow::anyhow!("Failed to input packet: {:?}", e))?;
        Ok(())
    }

    pub fn conv(&self) -> u32 {
        self.kcp().conv()
    }
}

#[cfg(test)]
mod kcp_session_tests {
    use super::*;
    use tokio::time::sleep;

    fn deliver_a_to_b(a: &KcpSession, b: &KcpSession) {
        while let Some(packet) = a.poll_output_packet() {
            b.input_packet(&packet).unwrap();
        }
    }

    fn deliver_a_to_b_lossy(a: &KcpSession, b: &KcpSession, loss_rate: f64) {
        let mut packets = Vec::new();

        while let Some(packet) = a.poll_output_packet() {
            if rand::random::<f64>() >= loss_rate {
                packets.push(packet);
            }
        }
        for i in 0..packets.len() {
            if i + 1 < packets.len() && rand::random::<f64>() < 0.5 {
                packets.swap(i, i + 1);
            }
        }
        for packet in packets {
            b.input_packet(&packet).unwrap();
        }
    }

    #[tokio::test]
    async fn test_kcp_session_short_message_exchange() -> Result<()> {
        let a = KcpSession::new(123, 1400);
        let b = KcpSession::new(123, 1400);

        a.send(b"Hello from A!").unwrap();
        sleep(Duration::from_millis(100)).await;
        deliver_a_to_b(&a, &b);
        sleep(Duration::from_millis(100)).await;
        let msg_from_a = b.recv().unwrap();
        assert_eq!(msg_from_a, b"Hello from A!");

        b.send(b"Hello from B!").unwrap();
        sleep(Duration::from_millis(100)).await;
        deliver_a_to_b(&b, &a);
        sleep(Duration::from_millis(100)).await;
        let msg_from_b = a.recv().unwrap();
        assert_eq!(msg_from_b, b"Hello from B!");
        Ok(())
    }

    #[tokio::test]
    async fn test_kcp_session_long_message_exchange() -> Result<()> {
        let a = KcpSession::new(123, 1400);
        let b = KcpSession::new(123, 1400);

        let long_msg_a = vec![b'A'; 5000];
        a.send(&long_msg_a).unwrap();
        sleep(Duration::from_millis(100)).await;
        deliver_a_to_b(&a, &b);
        sleep(Duration::from_millis(100)).await;
        let msg_from_a = b.recv().unwrap();
        assert_eq!(msg_from_a, long_msg_a);

        let long_msg_b = vec![b'B'; 5000];
        b.send(&long_msg_b).unwrap();
        sleep(Duration::from_millis(100)).await;
        deliver_a_to_b(&b, &a);
        sleep(Duration::from_millis(100)).await;
        let msg_from_b = a.recv().unwrap();
        assert_eq!(msg_from_b, long_msg_b);
        Ok(())
    }

    #[tokio::test]
    async fn test_kcp_session_with_packet_loss() -> Result<()> {
        let a = KcpSession::new(123, 1400);
        let b = KcpSession::new(123, 1400);

        let long_msg_a = vec![b'A'; 5000];
        a.send(&long_msg_a).unwrap();
        let long_msg_b = vec![b'B'; 5000];
        b.send(&long_msg_b).unwrap();
        for _ in 0..1000 {
            sleep(Duration::from_millis(1)).await;
            deliver_a_to_b_lossy(&a, &b, 0.1);
            deliver_a_to_b_lossy(&b, &a, 0.1);
        }
        let msg_from_a = b.recv().unwrap();
        assert_eq!(msg_from_a, long_msg_a);
        let msg_from_b = a.recv().unwrap();
        assert_eq!(msg_from_b, long_msg_b);
        Ok(())
    }

    #[tokio::test]
    async fn test_kcp_session_with_packet_loss_small_mtu() -> Result<()> {
        let a = KcpSession::new(123, 50);
        let b = KcpSession::new(123, 50);

        let long_msg_a = vec![b'A'; 5000];
        a.send(&long_msg_a).unwrap();
        let long_msg_b = vec![b'B'; 5000];
        b.send(&long_msg_b).unwrap();
        for _ in 0..1000 {
            sleep(Duration::from_millis(2)).await;
            deliver_a_to_b_lossy(&a, &b, 0.1);
            deliver_a_to_b_lossy(&b, &a, 0.1);
        }
        let msg_from_a = b.recv().unwrap();
        assert_eq!(msg_from_a, long_msg_a);
        let msg_from_b = a.recv().unwrap();
        assert_eq!(msg_from_b, long_msg_b);
        Ok(())
    }

    #[tokio::test]
    async fn test_kcp_session_with_packet_loss_small_mtu_large_message() -> Result<()> {
        let a = KcpSession::new(123, 140);
        let b = KcpSession::new(123, 140);

        let long_msg_a = vec![b'A'; 10000000];
        let long_msg_b = vec![b'B'; 10000000];
        let mut a_received = false;
        let mut b_received = false;
        let mut msg_from_a = Vec::new();
        let mut msg_from_b = Vec::new();

        let start_time = SystemTime::now();

        a.send(&long_msg_a).unwrap();
        b.send(&long_msg_b).unwrap();
        loop {
            sleep(Duration::from_millis(20)).await;
            deliver_a_to_b_lossy(&a, &b, 0.02);
            deliver_a_to_b_lossy(&b, &a, 0.02);
            if !a_received {
                if let Some(msg) = a.recv() {
                    msg_from_b = msg;
                    a_received = true;
                }
            }
            if !b_received {
                if let Some(msg) = b.recv() {
                    msg_from_a = msg;
                    b_received = true;
                }
            }
            if a_received && b_received {
                break;
            }
        }
        let elapsed = SystemTime::now().duration_since(start_time).unwrap();
        assert_eq!(msg_from_b, long_msg_b);
        assert_eq!(msg_from_a, long_msg_a);

        println!(
            "Transferred 10 MB from A to B and 10 MB from B to A in {:.2} ms",
            elapsed.as_secs_f64() * 1000.0
        );
        println!(
            "Estimated bandwidth: {:.2} Mbps",
            (msg_from_a.len() + msg_from_b.len()) as f64 * 8.0
                / elapsed.as_secs_f64()
                / 1_000_000.0
                / 2.0
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_kcp_session_with_packet_small_mtu_large_message() -> Result<()> {
        let a = KcpSession::new(123, 140);
        let b = KcpSession::new(123, 140);

        let long_msg_a = vec![b'A'; 10000000];
        let long_msg_b = vec![b'B'; 10000000];
        let mut a_received = false;
        let mut b_received = false;
        let mut msg_from_a = Vec::new();
        let mut msg_from_b = Vec::new();

        let start_time = SystemTime::now();

        a.send(&long_msg_a).unwrap();
        b.send(&long_msg_b).unwrap();
        loop {
            sleep(Duration::from_millis(20)).await;
            deliver_a_to_b(&a, &b);
            deliver_a_to_b(&b, &a);
            if !a_received {
                if let Some(msg) = a.recv() {
                    msg_from_b = msg;
                    a_received = true;
                }
            }
            if !b_received {
                if let Some(msg) = b.recv() {
                    msg_from_a = msg;
                    b_received = true;
                }
            }
            if a_received && b_received {
                break;
            }
        }
        let elapsed = SystemTime::now().duration_since(start_time).unwrap();
        assert_eq!(msg_from_b, long_msg_b);
        assert_eq!(msg_from_a, long_msg_a);

        println!(
            "Transferred 10 MB from A to B and 10 MB from B to A in {:.2} ms",
            elapsed.as_secs_f64() * 1000.0
        );
        println!(
            "Estimated bandwidth: {:.2} Mbps",
            (msg_from_a.len() + msg_from_b.len()) as f64 * 8.0
                / elapsed.as_secs_f64()
                / 1_000_000.0
                / 2.0
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_respect_mtu() -> Result<()> {
        let long_msg = vec![b'A'; 5000];

        for packet_size in (50..=1400).step_by(50) {
            let a = KcpSession::new(123, packet_size);
            let b = KcpSession::new(123, packet_size);
            a.send(&long_msg).unwrap();
            loop {
                sleep(Duration::from_millis(1)).await;
                while let Some(packet) = a.poll_output_packet() {
                    assert!(packet.len() <= packet_size);
                    b.input_packet(&packet).unwrap();
                }
                deliver_a_to_b(&b, &a);
                if let Some(msg) = b.recv() {
                    assert_eq!(msg, long_msg);
                    break;
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_conv() -> Result<()> {
        let a = KcpSession::new(123, 1400);
        let b = KcpSession::new(456, 1400);
        assert_eq!(a.conv(), 123);
        assert_eq!(b.conv(), 456);
        Ok(())
    }
}
