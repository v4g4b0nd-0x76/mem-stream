use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub struct StreamClient {
    stream: TcpStream,
    read_buf: BytesMut,
}

impl StreamClient {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(StreamClient {
            stream,
            read_buf: BytesMut::with_capacity(8192),
        })
    }

    async fn send_recv(&mut self, frame: &[u8]) -> anyhow::Result<Vec<u8>> {
        let start = std::time::Instant::now();
        tracing::info!("sending frame of length {}", frame.len());
            
        let body_len = frame.len() as u32;
        self.stream.write_all(&body_len.to_le_bytes()).await?;
        self.stream.write_all(frame).await?;
        self.stream.flush().await?;

        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let resp_len = u32::from_le_bytes(len_buf) as usize;
        let mut resp = vec![0u8; resp_len];
        self.stream.read_exact(&mut resp).await?;
        let duration = start.elapsed();
        tracing::info!("received frame of length {} in {:?}", resp_len, duration);

        Ok(resp)
    }

    fn build_cmd_with_group(tag: u8, group: &str) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 2 + group.len());
        buf.push(tag);
        buf.extend_from_slice(&(group.len() as u16).to_le_bytes());
        buf.extend_from_slice(group.as_bytes());
        buf
    }

    pub async fn create_group(&mut self, name: &str) -> anyhow::Result<Result<(), String>> {
        let frame = Self::build_cmd_with_group(0x01, name);
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    pub async fn drop_group(&mut self, name: &str) -> anyhow::Result<Result<(), String>> {
        let frame = Self::build_cmd_with_group(0x02, name);
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    pub async fn add(
        &mut self,
        group: &str,
        timestamp: u64,
        payload: &[u8],
    ) -> anyhow::Result<Result<u64, String>> {
        let mut frame = Vec::with_capacity(1 + 2 + group.len() + 8 + 4 + payload.len());
        frame.push(0x03);
        frame.extend_from_slice(&(group.len() as u16).to_le_bytes());
        frame.extend_from_slice(group.as_bytes());
        frame.extend_from_slice(&timestamp.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(payload);

        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 9 {
            Ok(Ok(u64::from_le_bytes(resp[1..9].try_into().unwrap())))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    pub async fn add_range(
        &mut self,
        group: &str,
        entries: &[(u64, &[u8])],
    ) -> anyhow::Result<Result<(u64, u64), String>> {
        let mut frame = Vec::new();
        frame.push(0x04);
        frame.extend_from_slice(&(group.len() as u16).to_le_bytes());
        frame.extend_from_slice(group.as_bytes());
        frame.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for &(ts, payload) in entries {
            frame.extend_from_slice(&ts.to_le_bytes());
            frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            frame.extend_from_slice(payload);
        }

        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 17 {
            let first = u64::from_le_bytes(resp[1..9].try_into().unwrap());
            let last = u64::from_le_bytes(resp[9..17].try_into().unwrap());
            Ok(Ok((first, last)))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    pub async fn read(
        &mut self,
        group: &str,
        id: u64,
    ) -> anyhow::Result<Result<(u64, u64, Vec<u8>), String>> {
        let mut frame = Self::build_cmd_with_group(0x05, group);
        frame.extend_from_slice(&id.to_le_bytes());

        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 21 {
            let entry_id = u64::from_le_bytes(resp[1..9].try_into().unwrap());
            let ts = u64::from_le_bytes(resp[9..17].try_into().unwrap());
            let plen = u32::from_le_bytes(resp[17..21].try_into().unwrap()) as usize;
            let payload = resp[21..21 + plen].to_vec();
            Ok(Ok((entry_id, ts, payload)))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    pub async fn read_range(
        &mut self,
        group: &str,
        start: u64,
        end: u64,
    ) -> anyhow::Result<Result<Vec<(u64, u64, Vec<u8>)>, String>> {
        let mut frame = Self::build_cmd_with_group(0x06, group);
        frame.extend_from_slice(&start.to_le_bytes());
        frame.extend_from_slice(&end.to_le_bytes());

        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 5 {
            let count = u32::from_le_bytes(resp[1..5].try_into().unwrap()) as usize;
            let mut entries = Vec::with_capacity(count);
            let mut pos = 5;
            for _ in 0..count {
                let id = u64::from_le_bytes(resp[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let ts = u64::from_le_bytes(resp[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let plen = u32::from_le_bytes(resp[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let payload = resp[pos..pos + plen].to_vec();
                pos += plen;
                entries.push((id, ts, payload));
            }
            Ok(Ok(entries))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    pub async fn remove(
        &mut self,
        group: &str,
        up_to_id: u64,
    ) -> anyhow::Result<Result<(), String>> {
        let mut frame = Self::build_cmd_with_group(0x07, group);
        frame.extend_from_slice(&up_to_id.to_le_bytes());
        let resp = self.send_recv(&frame).await?;
        Ok(Self::check_status(&resp))
    }

    pub async fn list_groups(&mut self) -> anyhow::Result<Result<Vec<String>, String>> {
        let frame = vec![0x08];
        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 5 {
            let count = u32::from_le_bytes(resp[1..5].try_into().unwrap()) as usize;
            let mut groups = Vec::with_capacity(count);
            let mut pos = 5;
            for _ in 0..count {
                let name_len =
                    u16::from_le_bytes(resp[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                let name = String::from_utf8_lossy(&resp[pos..pos + name_len]).to_string();
                pos += name_len;
                groups.push(name);
            }
            Ok(Ok(groups))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    pub async fn group_stats(
        &mut self,
        group: &str,
    ) -> anyhow::Result<Result<(u64, u32, u64), String>> {
        let frame = Self::build_cmd_with_group(0x09, group);
        let resp = self.send_recv(&frame).await?;
        if resp[0] == 0x00 && resp.len() >= 21 {
            let entries = u64::from_le_bytes(resp[1..9].try_into().unwrap());
            let segments = u32::from_le_bytes(resp[9..13].try_into().unwrap());
            let next_id = u64::from_le_bytes(resp[13..21].try_into().unwrap());
            Ok(Ok((entries, segments, next_id)))
        } else {
            Ok(Err(Self::extract_err(&resp)))
        }
    }

    fn check_status(resp: &[u8]) -> Result<(), String> {
        if resp.is_empty() {
            return Err("empty response".into());
        }
        if resp[0] == 0x00 {
            Ok(())
        } else {
            Err(Self::extract_err(resp))
        }
    }

    fn extract_err(resp: &[u8]) -> String {
        if resp.len() < 4 {
            return "unknown error".into();
        }
        let msg_len = u16::from_le_bytes([resp[1], resp[2]]) as usize;
        if resp.len() >= 3 + msg_len {
            String::from_utf8_lossy(&resp[3..3 + msg_len]).to_string()
        } else {
            "malformed error response".into()
        }
    }
}
