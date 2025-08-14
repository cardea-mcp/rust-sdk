use anyhow::Result;
use rmcp::{
    ServiceExt,
    model::{CallToolRequestParam, ClientCapabilities, ClientInfo, Implementation},
    transport::sse_client::SseClientAuto,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("info,{}=debug", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <server_did>", args[0]);
        std::process::exit(1);
    }
    let server_did = &args[1];
    let client = SseClientAuto::new(
        "client",
        server_did,
        Some(rmcp::transport::common::tmcp::TmcpSettings::default()),
    )
    .await?;

    tracing::info!("Resolved server endpoint: {}", client.sse_endpoint);

    let client_info = ClientInfo {
        protocol_version: Default::default(),
        capabilities: ClientCapabilities::default(),
        client_info: Implementation {
            name: "test sse tmcp client".to_string(),
            version: "0.0.1".to_string(),
        },
    };
    let client = client_info.serve(client.transport).await.inspect_err(|e| {
        tracing::error!("client error: {:?}", e);
    })?;

    let server_info = client.peer_info();
    tracing::info!("Connected to server: {server_info:#?}");

    let tools = client.list_tools(Default::default()).await?;
    tracing::info!("Available tools: {tools:#?}");

    let tool_result = client
        .call_tool(CallToolRequestParam {
            name: "increment".into(),
            arguments: serde_json::json!({}).as_object().cloned(),
        })
        .await?;
    tracing::info!("Tool result: {tool_result:#?}");
    client.cancel().await?;
    Ok(())
}
