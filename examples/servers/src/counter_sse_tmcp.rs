use rmcp::transport::sse_server::{SseServer, SseServerConfig};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
mod common;
use common::counter::Counter;

const BIND_ADDRESS: &str = "127.0.0.1:8001";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use rmcp::transport::common::tmcp::{TmcpIdentityManager, TmcpSettings};
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".to_string().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let mut settings = TmcpSettings::default();
    settings.transport = format!("sse://{}", BIND_ADDRESS);
    use std::sync::Arc;
    let manager = Arc::new(TmcpIdentityManager::new("counter-demo-server", settings).await?);
    let config = SseServerConfig {
        bind: BIND_ADDRESS.parse()?,
        sse_path: "/sse".to_string(),
        post_path: "/message".to_string(),
        ct: tokio_util::sync::CancellationToken::new(),
        sse_keep_alive: None,
        manager: Some(manager.clone()),
    };

    let (sse_server, mut router) = SseServer::new(config);
    use axum::{response::Redirect, routing::get};
    router = router.route("/", get(|| async { Redirect::temporary("/sse") }));

    let listener = tokio::net::TcpListener::bind(sse_server.config.bind).await?;
    let ct = sse_server.config.ct.child_token();

    let server = axum::serve(listener, router).with_graceful_shutdown(async move {
        ct.cancelled().await;
        tracing::info!("counter_sse_tmcp server cancelled");
    });

    tokio::spawn(async move {
        if let Err(e) = server.await {
            tracing::error!(error = %e, "counter_sse_tmcp server shutdown with error");
        }
    });

    let ct = sse_server.with_service(Counter::new);

    println!("Using existing DID: {}", manager.get_did());
    println!(
        "Counter SSE TMCP server running at http://{}/sse",
        BIND_ADDRESS
    );
    println!("POST messages to http://{}/message", BIND_ADDRESS);

    tokio::signal::ctrl_c().await?;
    ct.cancel();
    Ok(())
}
