use crate::server::Server;

mod codec;
mod groups;
mod proto;
mod seg_log;
mod server;
#[tokio::main]
async fn main() {
    let srv : Server = Server::new();
    if let Err(e) = srv.start().await {
        eprintln!("Server error: {}", e);
    }
}
