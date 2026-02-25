use crate::client::client::StreamClient;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

pub struct ClientPool {
    slots: tokio::sync::mpsc::Sender<StreamClient>,
    recv: Mutex<tokio::sync::mpsc::Receiver<StreamClient>>,
    addr: String,
    active: AtomicU64,
    #[allow(dead_code)]
    max_size: u64,
}

pub struct PooledClient<'a> {
    client: Option<StreamClient>,
    pool: &'a ClientPool,
    poisoned: bool,
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


        let max_attempts = self.active.load(Ordering::Relaxed) as usize + 1;
        for _ in 0..max_attempts {
            let client = match rx.try_recv() {
                Ok(c) => c,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    rx.recv()
                        .await
                        .ok_or_else(|| anyhow::anyhow!("pool closed"))?
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    anyhow::bail!("pool channel disconnected");
                }
            };
    
            if client.is_alive() {
                return Ok(PooledClient {
                    client: Some(client),
                    pool: self,
                    poisoned: false,
                });
            }

    
            drop(client);
            self.spawn_replacement();
        }


        let client = StreamClient::connect(&self.addr).await?;
        Ok(PooledClient {
            client: Some(client),
            pool: self,
            poisoned: false,
        })
    }

    fn spawn_replacement(&self) {
        let addr = self.addr.clone();
        let tx = self.slots.clone();
        tokio::spawn(async move {
            if let Ok(client) = StreamClient::connect(&addr).await {
                let _ = tx.send(client).await;
            }
        });
    }
    #[allow(dead_code)]
    async fn release(&self, client: StreamClient) {

        if client.is_alive() {
            let _ = self.slots.try_send(client);
        } else {
            self.spawn_replacement();
        }
    }
}

impl<'a> PooledClient<'a> {
    pub fn conn(&mut self) -> &mut StreamClient {
        self.client.as_mut().unwrap()
    }

    #[allow(dead_code)]
    pub fn poison(&mut self) {
        self.poisoned = true;
    }
    #[allow(dead_code)]
    pub async fn discard(mut self) {
        self.client.take();
        self.pool.spawn_replacement();
    }
}

impl<'a> Drop for PooledClient<'a> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            if self.poisoned || !client.is_alive() {
        
                let addr = self.pool.addr.clone();
                let tx = self.pool.slots.clone();
                tokio::spawn(async move {
                    drop(client);
                    if let Ok(new_client) = StreamClient::connect(&addr).await {
                        let _ = tx.send(new_client).await;
                    }
                });
            } else {
                let tx = self.pool.slots.clone();
                let _ = tx.try_send(client);
            }
        }
    }
}
