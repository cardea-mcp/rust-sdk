use std::{collections::HashMap, io, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Extension, Router,
    extract::{NestedPath, Query, State},
    http::{StatusCode, request::Parts},
    response::{
        Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use bytes::Bytes;
use futures::{Sink, SinkExt, Stream};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::{CancellationToken, PollSender};
use tracing::Instrument;

use crate::{
    RoleServer, Service,
    model::ClientJsonRpcMessage,
    service::{RxJsonRpcMessage, TxJsonRpcMessage, serve_directly_with_ct},
    transport::{
        common::server_side_http::{DEFAULT_AUTO_PING_INTERVAL, SessionId, session_id},
        tsp_utils::get_client_did,
    },
};
type TxStore =
    Arc<tokio::sync::RwLock<HashMap<SessionId, tokio::sync::mpsc::Sender<ClientJsonRpcMessage>>>>;
pub type TransportReceiver = ReceiverStream<RxJsonRpcMessage<RoleServer>>;

#[derive(Clone, Debug)]
struct App {
    txs: TxStore,
    transport_tx: tokio::sync::mpsc::UnboundedSender<SseServerTransport>,
    sse_ping_interval: Duration,
    manager: Option<std::sync::Arc<crate::transport::common::tmcp::TmcpIdentityManager>>,
}

impl App {
    pub fn new(
        sse_ping_interval: Duration,
        manager: Option<std::sync::Arc<crate::transport::common::tmcp::TmcpIdentityManager>>,
    ) -> (
        Self,
        tokio::sync::mpsc::UnboundedReceiver<SseServerTransport>,
    ) {
        let (transport_tx, transport_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                txs: Default::default(),
                transport_tx,
                sse_ping_interval,
                manager,
            },
            transport_rx,
        )
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostEventQuery {
    pub session_id: String,
}

use axum::http::HeaderMap;

async fn post_event_handler(
    State(app): State<App>,
    Query(query): Query<HashMap<String, String>>,
    parts: Parts,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let session_id_from_query = parts.uri.query().and_then(|q| {
        url::form_urlencoded::parse(q.as_bytes())
            .find(|(k, _)| k == "sessionId")
            .map(|(_, v)| v.into_owned())
    });
    let client_key = if let Some(session_id) = session_id_from_query {
        Arc::from(session_id)
    } else if let Some(did) = query.get("did") {
        let did_decoded = percent_encoding::percent_decode_str(did)
            .decode_utf8_lossy()
            .to_string();
        Arc::from(did_decoded)
    } else if let Some(session_id) = query.get("session_id") {
        Arc::from(session_id.clone())
    } else {
        Arc::from("")
    };

    let parse_result = crate::transport::tsp_utils::parse_message_by_content_type(
        content_type,
        http_body_util::Full::new(body.clone()),
        app.manager.clone(),
        &parts,
    )
    .await;
    let mut message: ClientJsonRpcMessage = parse_result.unwrap();

    let tx = {
        let rg = app.txs.write().await;
        if !rg.contains_key(&client_key) {
            if rg.contains_key(&Arc::from("")) {
                rg.get(&Arc::from("")).unwrap().clone()
            } else {
                return Err(StatusCode::NOT_FOUND);
            }
        } else {
            rg.get(&client_key).unwrap().clone()
        }
    };
    tracing::debug!(
        session_id=?client_key,
        ?parts,
        ?message,
        "new client message"
    );
    message.insert_extension(parts);
    if tx.send(message).await.is_err() {
        return Err(StatusCode::GONE);
    }
    Ok(StatusCode::ACCEPTED)
}

async fn sse_handler(
    State(app): State<App>,
    nested_path: Option<Extension<NestedPath>>,
    parts: Parts,
) -> Result<Sse<impl Stream<Item = Result<Event, io::Error>>>, Response<String>> {
    let session = session_id();
    tracing::info!(%session, ?parts, "sse connection");
    use tokio_stream::{StreamExt, wrappers::ReceiverStream};
    use tokio_util::sync::PollSender;
    let (from_client_tx, from_client_rx) = tokio::sync::mpsc::channel(64);
    let (to_client_tx, to_client_rx) = tokio::sync::mpsc::channel(64);

    let client_did = get_client_did(&parts);

    let did_decoded = percent_encoding::percent_decode_str(&client_did)
        .decode_utf8_lossy()
        .to_string();
    if !client_did.is_empty() {
        app.txs
            .write()
            .await
            .insert(Arc::from(did_decoded), from_client_tx);
    } else {
        app.txs.write().await.insert(Arc::from(""), from_client_tx);
    }
    let stream = ReceiverStream::new(from_client_rx);
    let to_client_tx_clone = to_client_tx.clone();
    let sink = PollSender::new(to_client_tx);
    let transport = SseServerTransport {
        stream,
        sink,
        session_id: session.clone(),
        tx_store: app.txs.clone(),
    };
    let transport_send_result = app.transport_tx.send(transport);
    if transport_send_result.is_err() {
        tracing::warn!("send transport out error");
        let mut response =
            Response::new("fail to send out transport, it seems server is closed".to_string());
        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        return Err(response);
    }
    let session_id = session.clone();
    let tx_store = app.txs.clone();
    tokio::spawn(async move {
        to_client_tx_clone.closed().await;
        let mut txs = tx_store.write().await;
        txs.remove(&session_id);
        tracing::debug!(%session_id, "Closed session and cleaned up resources");
    });
    let ping_interval = app.sse_ping_interval;

    use std::pin::Pin;

    use futures::Stream as FuturesStream;

    let stream: Pin<Box<dyn FuturesStream<Item = Result<Event, io::Error>> + Send>> =
        if let Some(manager) = app.manager.clone() {
            if !client_did.is_empty() {
                let did_decoded = percent_encoding::percent_decode_str(&client_did)
                    .decode_utf8_lossy()
                    .to_string();
                match manager.get_connection(&did_decoded).await {
                    Ok(tmcp_conn) => {
                        let tmcp: std::sync::Arc<_> = std::sync::Arc::new(tmcp_conn);
                        let nested_path_str =
                            nested_path.as_deref().map(NestedPath::as_str).unwrap_or("");
                        let endpoint_path =
                            format!("{}{}?did={}", nested_path_str, "/message", client_did);
                        use crate::transport::tsp_utils::serialize_event;

                        let endpoint_json = serialize_event("endpoint", &endpoint_path);
                        let sealed = tmcp
                            .seal_message(&endpoint_json)
                            .unwrap_or_else(|_| "seal_message error".to_string());
                        Box::pin(
                            futures::stream::once(futures::future::ok(
                                Event::default().event("endpoint").data(sealed),
                            ))
                            .chain(
                                ReceiverStream::new(to_client_rx).map({
                                    let tmcp = tmcp.clone();
                                    move |message| {
                                        let json =
                                            serde_json::to_string(&message).map_err(|e| {
                                                io::Error::new(io::ErrorKind::InvalidData, e)
                                            })?;
                                        let message_json = serialize_event("message", &json);
                                        let sealed =
                                            tmcp.seal_message(&message_json).map_err(|e| {
                                                io::Error::new(io::ErrorKind::InvalidData, e)
                                            })?;
                                        Ok(Event::default().event("message").data(&sealed))
                                    }
                                }),
                            ),
                        )
                    }
                    Err(_) => Box::pin(
                        futures::stream::once(futures::future::ok::<Event, io::Error>(
                            Event::default().event("endpoint").data(""),
                        ))
                        .chain(ReceiverStream::new(to_client_rx).map(
                            move |message| {
                                let json = serde_json::to_string(&message)
                                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                                Ok(Event::default().event("message").data(json))
                            },
                        )),
                    ),
                }
            } else {
                Box::pin(
                    futures::stream::once(futures::future::ok::<Event, io::Error>({
                        let nested_path_str =
                            nested_path.as_deref().map(NestedPath::as_str).unwrap_or("");
                        Event::default()
                            .event("endpoint")
                            .data(format!("{}{}", nested_path_str, "/message"))
                    }))
                    .chain(ReceiverStream::new(to_client_rx).map(
                        move |message| {
                            let json = serde_json::to_string(&message)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                            Ok(Event::default().event("message").data(json))
                        },
                    )),
                )
            }
        } else {
            Box::pin(
                futures::stream::once(futures::future::ok::<Event, io::Error>({
                    let nested_path_str =
                        nested_path.as_deref().map(NestedPath::as_str).unwrap_or("");
                    Event::default()
                        .event("endpoint")
                        .data(format!("{}{}", nested_path_str, "/message"))
                }))
                .chain(ReceiverStream::new(to_client_rx).map(move |message| {
                    let json = serde_json::to_string(&message)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    Ok(Event::default().event("message").data(json))
                })),
            )
        };

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(ping_interval)))
}

pub struct SseServerTransport {
    stream: ReceiverStream<RxJsonRpcMessage<RoleServer>>,
    sink: PollSender<TxJsonRpcMessage<RoleServer>>,
    session_id: SessionId,
    tx_store: TxStore,
}

impl Sink<TxJsonRpcMessage<RoleServer>> for SseServerTransport {
    type Error = io::Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.sink
            .poll_ready_unpin(cx)
            .map_err(std::io::Error::other)
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: TxJsonRpcMessage<RoleServer>,
    ) -> Result<(), Self::Error> {
        self.sink
            .start_send_unpin(item)
            .map_err(std::io::Error::other)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.sink
            .poll_flush_unpin(cx)
            .map_err(std::io::Error::other)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let inner_close_result = self
            .sink
            .poll_close_unpin(cx)
            .map_err(std::io::Error::other);
        if inner_close_result.is_ready() {
            let session_id = self.session_id.clone();
            let tx_store = self.tx_store.clone();
            tokio::spawn(async move {
                tx_store.write().await.remove(&session_id);
            });
        }
        inner_close_result
    }
}

impl Stream for SseServerTransport {
    type Item = RxJsonRpcMessage<RoleServer>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures::StreamExt;
        self.stream.poll_next_unpin(cx)
    }
}

#[derive(Debug, Clone)]
pub struct SseServerConfig {
    pub bind: SocketAddr,
    pub sse_path: String,
    pub post_path: String,
    pub ct: CancellationToken,
    pub sse_keep_alive: Option<Duration>,
    pub manager: Option<std::sync::Arc<crate::transport::common::tmcp::TmcpIdentityManager>>,
}

#[derive(Debug)]
pub struct SseServer {
    transport_rx: tokio::sync::mpsc::UnboundedReceiver<SseServerTransport>,
    pub config: SseServerConfig,
}

impl SseServer {
    pub async fn serve(bind: SocketAddr) -> io::Result<Self> {
        Self::serve_with_config(SseServerConfig {
            bind,
            sse_path: "/sse".to_string(),
            post_path: "/message".to_string(),
            ct: CancellationToken::new(),
            sse_keep_alive: None,
            manager: None,
        })
        .await
    }
    pub async fn serve_with_config(config: SseServerConfig) -> io::Result<Self> {
        let (sse_server, service) = Self::new(config);
        let listener = tokio::net::TcpListener::bind(sse_server.config.bind).await?;
        let ct = sse_server.config.ct.child_token();
        let server = axum::serve(listener, service).with_graceful_shutdown(async move {
            ct.cancelled().await;
            tracing::info!("sse server cancelled");
        });
        tokio::spawn(
            async move {
                if let Err(e) = server.await {
                    tracing::error!(error = %e, "sse server shutdown with error");
                }
            }
            .instrument(tracing::info_span!("sse-server", bind_address = %sse_server.config.bind)),
        );
        Ok(sse_server)
    }

    pub fn new(config: SseServerConfig) -> (SseServer, Router) {
        let (app, transport_rx) = App::new(
            config.sse_keep_alive.unwrap_or(DEFAULT_AUTO_PING_INTERVAL),
            config.manager.clone(),
        );
        let router = Router::new()
            .route(&config.sse_path, get(sse_handler))
            .route(&config.post_path, post(post_event_handler))
            .route(
                &format!("{}/", config.post_path.trim_end_matches('/')),
                post(post_event_handler),
            )
            .with_state(app);

        let server = SseServer {
            transport_rx,
            config,
        };

        (server, router)
    }

    pub fn with_service<S, F>(mut self, service_provider: F) -> CancellationToken
    where
        S: Service<RoleServer>,
        F: Fn() -> S + Send + 'static,
    {
        use crate::service::ServiceExt;
        let ct = self.config.ct.clone();
        tokio::spawn(async move {
            while let Some(transport) = self.next_transport().await {
                let service = service_provider();
                let ct = self.config.ct.child_token();
                tokio::spawn(async move {
                    let server = service
                        .serve_with_ct(transport, ct)
                        .await
                        .map_err(std::io::Error::other)?;
                    server.waiting().await?;
                    tokio::io::Result::Ok(())
                });
            }
        });
        ct
    }

    /// This allows you to skip the initialization steps for incoming request.
    pub fn with_service_directly<S, F>(mut self, service_provider: F) -> CancellationToken
    where
        S: Service<RoleServer>,
        F: Fn() -> S + Send + 'static,
    {
        let ct = self.config.ct.clone();
        tokio::spawn(async move {
            while let Some(transport) = self.next_transport().await {
                let service = service_provider();
                let ct = self.config.ct.child_token();
                tokio::spawn(async move {
                    let server = serve_directly_with_ct(service, transport, None, ct);
                    server.waiting().await?;
                    tokio::io::Result::Ok(())
                });
            }
        });
        ct
    }

    pub fn cancel(&self) {
        self.config.ct.cancel();
    }

    pub async fn next_transport(&mut self) -> Option<SseServerTransport> {
        self.transport_rx.recv().await
    }
}

impl Stream for SseServer {
    type Item = SseServerTransport;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.transport_rx.poll_recv(cx)
    }
}
