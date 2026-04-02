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
    output_packet_buf: OutputPacketBuf,
    _is_closed: mpsc::Sender<()>,
}

impl KcpSession {
    pub fn new(conv: u32, mtu: usize) -> Self {
        let output = OutputPacketBuf(Arc::new(Mutex::new(Vec::<Vec<u8>>::new())));
        let mut kcp = Kcp::new(conv, output.clone());
        kcp.set_nodelay(true, 20, 2, true);
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
            output_packet_buf: output,
            _is_closed: tx,
        }
    }

    fn kcp(&self) -> std::sync::MutexGuard<'_, Kcp<OutputPacketBuf>> {
        self.kcp_ptr.lock().unwrap()
    }

    pub fn send(&self, data: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn recv(&self) -> Option<Vec<u8>> {
        todo!()
    }

    pub fn poll_output_packet(&self) -> Option<Vec<u8>> {
        todo!()
    }

    pub fn input_packet(&self, packet: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn conv(&self) -> u32 {
        self.kcp().conv()
    }
}
