use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
mod common;
use std::{sync::Arc, time::Duration};

use axum::Router;
use common::counter::Counter;
use rmcp::transport::common::tmcp::{TmcpIdentityManager, TmcpSettings};

const BIND_ADDRESS: &str = "127.0.0.1:8002";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".to_string().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let mut settings = TmcpSettings::default();
    settings.transport = format!("http://{}", BIND_ADDRESS);

    let manager = Arc::new(TmcpIdentityManager::new("counter-demo-server-http", settings).await?);
    let config = StreamableHttpServerConfig {
        sse_keep_alive: Some(Duration::from_secs(15)),
        stateful_mode: true,
        manager: Some(manager.clone()),
    };

    let service = StreamableHttpService::new(
        || Ok(Counter::new()),
        LocalSessionManager::default().into(),
        config,
    );

    let router = Router::new().fallback_service(service);
    let tcp_listener = tokio::net::TcpListener::bind(BIND_ADDRESS).await?;
    println!("Using existing DID: {}", manager.get_did());
    println!(
        "Counter StreamHTTP TMCP server running at http://{}/mcp",
        BIND_ADDRESS
    );
    println!("POST messages to http://{}/mcp", BIND_ADDRESS);

    let _ = axum::serve(tcp_listener, router)
        .with_graceful_shutdown(async { tokio::signal::ctrl_c().await.unwrap() })
        .await;
    Ok(())
}
