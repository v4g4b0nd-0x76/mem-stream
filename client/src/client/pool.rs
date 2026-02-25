use crate::client::client::StreamClient;
use anyhow::Ok;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

pub struct ClientPool {
    slots: tokio::sync::mpsc::Sender<StreamClient>,
    recv: Mutex<tokio::sync::mpsc::Receiver<StreamClient>>,
    addr: String,
    active: AtomicU64,
    max_size: u64,
}
pub struct PooledClient<'a> {
    client: Option<StreamClient>,
    pool: &'a ClientPool,
}
impl ClientPool {
    pub async fn new(addr: &str, size: usize) -> anyhow::Result<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel(size);
        for _ in 0..size {
            let client = StreamClient::connect(addr).await?;
            tx.send(client).await?;
        }
        Ok(Self {
            slots: tx,
            recv: Mutex::new(rx),
            addr: addr.to_string(),
            active: AtomicU64::new(size as u64),
            max_size: size as u64,
        })
    }

    pub async fn acquire(&self) -> anyhow::Result<PooledClient<'_>> {
        let mut rx = self.recv.lock().await;
        match rx.try_recv() {
            Result::Ok(client) => Ok(PooledClient {
                client: Some(client),
                pool: self,
            }),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                let client = rx
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("pool closed"))?;
                Ok(PooledClient {
                    client: Some(client),
                    pool: self,
                })
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                anyhow::bail!("pool channel disconnected");
            }
        }
    }
       async fn release(&self, client: StreamClient) {
        let _ = self.slots.try_send(client);
    }
      async fn replace(&self) {
        if let Result::Ok(client) = StreamClient::connect(&self.addr).await {
            let _ = self.slots.try_send(client);
        } else {
            self.active.fetch_sub(1, Ordering::Relaxed);
        }
    }
}


impl <'a> PooledClient<'a> {
    pub fn conn(&mut self) -> &mut StreamClient {
        self.client.as_mut().unwrap()
    }
    pub async fn discard(mut self) {
        self.client.take();
        self.pool.replace().await;
    }
}

impl<'a> Drop for PooledClient<'a> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let tx = self.pool.slots.clone();
            let _ = tx.try_send(client);
        }
    }
}