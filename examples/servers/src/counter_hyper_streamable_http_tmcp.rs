mod common;
use std::sync::Arc;

use common::counter::Counter;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
    service::TowerToHyperService,
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use rmcp::transport::common::tmcp::{TmcpIdentityManager, TmcpSettings};

    let mut settings = TmcpSettings::default();
    settings.transport = "http://[::1]:8080".to_string();
    let manager = Arc::new(TmcpIdentityManager::new("counter-hyper-demo-server", settings).await?);

    let config = StreamableHttpServerConfig {
        sse_keep_alive: Some(std::time::Duration::from_secs(15)),
        stateful_mode: true,
        manager: Some(manager.clone()),
    };

    let service = TowerToHyperService::new(StreamableHttpService::new(
        || Ok(Counter::new()),
        LocalSessionManager::default().into(),
        config,
    ));
    let listener = tokio::net::TcpListener::bind("[::1]:8080").await?;
    println!("Using existing DID: {}", manager.get_did());
    println!("Counter Hyper Streamable HTTP TMCP server running at http://[::1]:8080");
    loop {
        let io = tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            accept = listener.accept() => {
                TokioIo::new(accept?.0)
            }
        };
        let service = service.clone();
        tokio::spawn(async move {
            let _result = Builder::new(TokioExecutor::default())
                .serve_connection(io, service)
                .await;
        });
    }
    Ok(())
}
