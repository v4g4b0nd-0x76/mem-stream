use std::{ env, sync::Arc};

use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::client::pool::ClientPool;

mod client;
mod api;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
      tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .init();

    let server_addr = env::var("SERVER_ADDR").unwrap_or_else(|_| "localhost:9090".to_string());
    let pool_size = env::var("POOL_SIZE").unwrap_or_else(|_| "10".to_string()).parse().unwrap_or(10);
    let http_addr = env::var("HTTP_ADDR").unwrap_or_else(|_| "localhost:8000".to_string());

    tracing::info!("connecting pool ({pool_size} conns) to {server_addr}...");
    let pool = ClientPool::new(&server_addr, pool_size).await?;
    let pool = Arc::new(pool);
    
    tracing::info!("starting HTTP server on {http_addr}...");
    let app = api::router::build_router(pool);
    let ln = TcpListener::bind(&http_addr).await?;
    axum::serve(ln, app).await?;
    Ok(())
}
