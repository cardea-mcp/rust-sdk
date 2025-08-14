use std::{borrow::Cow, sync::Arc, time::Duration};

use futures::{Stream, StreamExt, future::BoxFuture, stream::BoxStream};
pub use sse_stream::Error as SseError;
use sse_stream::Sse;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::common::{
    client_side_sse::{ExponentialBackoff, SseRetryPolicy, SseStreamReconnect},
    tmcp::{TmcpConnection, TmcpIdentityManager, TmcpSettings, resolve_server},
};
use crate::{
    RoleClient,
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::{
        common::client_side_sse::SseAutoReconnectStream,
        worker::{Worker, WorkerQuitReason, WorkerSendRequest, WorkerTransport},
    },
};

pub struct StreamableHttpClientAuto {
    pub identity: TmcpIdentityManager,
    pub tmcp_connection: TmcpConnection,
    pub url: String,
    pub transport: StreamableHttpClientTransport<
        crate::transport::common::reqwest::streamable_http_client::TmcpReqwestClient,
    >,
}

impl StreamableHttpClientAuto {
    pub async fn new(
        alias: &str,
        server_did: &str,
        tmcp_settings: Option<TmcpSettings>,
    ) -> anyhow::Result<Self> {
        let settings = tmcp_settings.unwrap_or_default();
        let identity = TmcpIdentityManager::new(alias, settings).await?;
        let tmcp_connection = identity.get_connection(server_did).await?;
        let url = resolve_server(server_did, Some(identity.did.as_str())).await?;
        if !url.starts_with("http://") && !url.starts_with("https://") {
            anyhow::bail!("Server does not use HTTP for transport: {}", url);
        }
        let transport = StreamableHttpClientTransport::with_client(
            crate::transport::common::reqwest::streamable_http_client::TmcpReqwestClient {
                client: reqwest::Client::new(),
                tmcp_connection: Some(tmcp_connection.clone()),
            },
            StreamableHttpClientTransportConfig::with_uri(url.clone()),
        );
        Ok(Self {
            identity,
            tmcp_connection,
            url,
            transport,
        })
    }
}

type BoxedSseStream = BoxStream<'static, Result<Sse, SseError>>;

#[derive(Error, Debug)]
pub enum StreamableHttpError<E: std::error::Error + Send + Sync + 'static> {
    #[error("SSE error: {0}")]
    Sse(#[from] SseError),
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Client error: {0}")]
    Client(E),
    #[error("unexpected end of stream")]
    UnexpectedEndOfStream,
    #[error("unexpected server response: {0}")]
    UnexpectedServerResponse(Cow<'static, str>),
    #[error("Unexpected content type: {0:?}")]
    UnexpectedContentType(Option<String>),
    #[error("Server does not support SSE")]
    ServerDoesNotSupportSse,
    #[error("Server does not support delete session")]
    ServerDoesNotSupportDeleteSession,
    #[error("Tokio join error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),
    #[error("Deserialize error: {0}")]
    Deserialize(#[from] serde_json::Error),
    #[error("Transport channel closed")]
    TransportChannelClosed,
    #[error("Missing session id in HTTP response")]
    MissingSessionIdInResponse,
    #[cfg(feature = "auth")]
    #[cfg_attr(docsrs, doc(cfg(feature = "auth")))]
    #[error("Auth error: {0}")]
    Auth(#[from] crate::transport::auth::AuthError),
}

impl From<reqwest::Error> for StreamableHttpError<reqwest::Error> {
    fn from(e: reqwest::Error) -> Self {
        StreamableHttpError::Client(e)
    }
}

#[derive(Debug, Clone, Error)]
pub enum StreamableHttpProtocolError {
    #[error("Missing session id in response")]
    MissingSessionIdInResponse,
}
pub enum StreamableHttpPostResponse {
    Accepted,
    Json(ServerJsonRpcMessage, Option<String>),
    Sse(BoxedSseStream, Option<String>),
}

impl std::fmt::Debug for StreamableHttpPostResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Accepted => write!(f, "Accepted"),
            Self::Json(arg0, arg1) => f.debug_tuple("Json").field(arg0).field(arg1).finish(),
            Self::Sse(_, arg1) => f.debug_tuple("Sse").field(arg1).finish(),
        }
    }
}

impl StreamableHttpPostResponse {
    pub async fn expect_initialized<E>(
        self,
        tmcp_connection: Option<&TmcpConnection>,
    ) -> Result<(ServerJsonRpcMessage, Option<String>), StreamableHttpError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Self::Json(message, session_id) => Ok((message, session_id)),
            Self::Sse(mut stream, session_id) => {
                let event =
                    stream
                        .next()
                        .await
                        .ok_or(StreamableHttpError::UnexpectedServerResponse(
                            "empty sse stream".into(),
                        ))??;
                let data = event.data.unwrap_or_default();
                tracing::debug!(
                    "expect_initialized: tmcp_connection is_some = {}",
                    tmcp_connection.is_some()
                );
                tracing::debug!("expect_initialized: sse data = {}", data);

                let message = if let Some(conn) = tmcp_connection {
                    tracing::debug!("expect_initialized: entering decode branch");
                    match conn.open_message(&data) {
                        Ok(decoded) => {
                            tracing::debug!("expect_initialized: decoded = {}", decoded);
                            serde_json::from_str::<ServerJsonRpcMessage>(&decoded)?
                        }
                        Err(e) => {
                            tracing::error!("expect_initialized: decode error = {:?}", e);
                            tracing::debug!("expect_initialized: fallback data = {}", data);
                            serde_json::from_str::<ServerJsonRpcMessage>(&data)?
                        }
                    }
                } else if let Ok(json) = serde_json::from_str::<serde_json::Value>(&data) {
                    if json.get("event") == Some(&serde_json::Value::String("message".to_string()))
                    {
                        if let Some(inner) = json.get("data").and_then(|v| v.as_str()) {
                            serde_json::from_str::<ServerJsonRpcMessage>(inner)?
                        } else {
                            serde_json::from_str::<ServerJsonRpcMessage>("null")?
                        }
                    } else {
                        serde_json::from_value::<ServerJsonRpcMessage>(json)?
                    }
                } else {
                    serde_json::from_str::<ServerJsonRpcMessage>(&data)?
                };
                Ok((message, session_id))
            }
            _ => Err(StreamableHttpError::UnexpectedServerResponse(
                "expect initialized, accepted".into(),
            )),
        }
    }

    pub fn expect_json<E>(self) -> Result<ServerJsonRpcMessage, StreamableHttpError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Self::Json(message, ..) => Ok(message),
            got => Err(StreamableHttpError::UnexpectedServerResponse(
                format!("expect json, got {got:?}").into(),
            )),
        }
    }

    pub fn expect_accepted<E>(self) -> Result<(), StreamableHttpError<E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        match self {
            Self::Accepted => Ok(()),
            got => Err(StreamableHttpError::UnexpectedServerResponse(
                format!("expect accepted, got {got:?}").into(),
            )),
        }
    }
}

pub trait StreamableHttpClient: Clone + Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        content: Option<String>,
    ) -> impl Future<Output = Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>>>
    + Send
    + '_;
    fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_header: Option<String>,
    ) -> impl Future<Output = Result<(), StreamableHttpError<Self::Error>>> + Send + '_;
    fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
    ) -> impl Future<
        Output = Result<
            BoxStream<'static, Result<Sse, SseError>>,
            StreamableHttpError<Self::Error>,
        >,
    > + Send
    + '_;
}

pub struct RetryConfig {
    pub max_times: Option<usize>,
    pub min_duration: Duration,
}

struct StreamableHttpClientReconnect<C> {
    pub client: C,
    pub session_id: Arc<str>,
    pub uri: Arc<str>,
}

impl<C: StreamableHttpClient> SseStreamReconnect for StreamableHttpClientReconnect<C> {
    type Error = StreamableHttpError<C::Error>;
    type Future = BoxFuture<'static, Result<BoxedSseStream, Self::Error>>;
    fn retry_connection(&mut self, last_event_id: Option<&str>) -> Self::Future {
        let client = self.client.clone();
        let uri = self.uri.clone();
        let session_id = self.session_id.clone();
        let last_event_id = last_event_id.map(|s| s.to_owned());
        Box::pin(async move {
            client
                .get_stream(uri, session_id, last_event_id, None)
                .await
        })
    }
}

#[derive(Debug, Clone)]
pub struct StreamableHttpClientWorker<C: StreamableHttpClient> {
    pub client: C,
    pub config: StreamableHttpClientTransportConfig,
    pub tmcp_connection: Option<TmcpConnection>,
}

impl<C: StreamableHttpClient + Default> StreamableHttpClientWorker<C> {
    pub fn new_simple(url: impl Into<Arc<str>>) -> Self {
        Self {
            client: C::default(),
            config: StreamableHttpClientTransportConfig {
                uri: url.into(),
                ..Default::default()
            },
            tmcp_connection: None,
        }
    }
}

impl<C: StreamableHttpClient> StreamableHttpClientWorker<C> {
    pub fn new(client: C, config: StreamableHttpClientTransportConfig) -> Self {
        Self {
            client,
            config,
            tmcp_connection: None,
        }
    }
}

impl<C: StreamableHttpClient> StreamableHttpClientWorker<C> {
    async fn execute_sse_stream(
        sse_stream: impl Stream<Item = Result<ServerJsonRpcMessage, StreamableHttpError<C::Error>>>
        + Send
        + 'static,
        sse_worker_tx: tokio::sync::mpsc::Sender<ServerJsonRpcMessage>,
        close_on_response: bool,
        ct: CancellationToken,
    ) -> Result<(), StreamableHttpError<C::Error>> {
        let mut sse_stream = std::pin::pin!(sse_stream);
        loop {
            let message = tokio::select! {
                event = sse_stream.next() => {
                    event
                }
                _ = ct.cancelled() => {
                    tracing::debug!("cancelled");
                    break;
                }
            };
            let Some(message) = message.transpose()? else {
                break;
            };
            let is_response = matches!(message, ServerJsonRpcMessage::Response(_));
            let yield_result = sse_worker_tx.send(message).await;
            if yield_result.is_err() {
                tracing::trace!("streamable http transport worker dropped, exiting");
                break;
            }
            if close_on_response && is_response {
                tracing::debug!("got response, closing sse stream");
                break;
            }
        }
        Ok(())
    }
}

impl<C> Worker for StreamableHttpClientWorker<C>
where
    C: StreamableHttpClient,
{
    type Role = RoleClient;
    type Error = StreamableHttpError<C::Error>;
    fn err_closed() -> Self::Error {
        StreamableHttpError::TransportChannelClosed
    }
    fn err_join(e: tokio::task::JoinError) -> Self::Error {
        StreamableHttpError::TokioJoinError(e)
    }
    fn config(&self) -> super::worker::WorkerConfig {
        super::worker::WorkerConfig {
            name: Some("StreamableHttpClientWorker".into()),
            channel_buffer_capacity: self.config.channel_buffer_capacity,
        }
    }
    async fn run(
        self,
        mut context: super::worker::WorkerContext<Self>,
    ) -> Result<(), WorkerQuitReason<Self::Error>> {
        let channel_buffer_capacity = self.config.channel_buffer_capacity;
        let (sse_worker_tx, mut sse_worker_rx) =
            tokio::sync::mpsc::channel::<ServerJsonRpcMessage>(channel_buffer_capacity);
        let config = self.config.clone();
        let transport_task_ct = context.cancellation_token.clone();
        let _drop_guard = transport_task_ct.clone().drop_guard();
        let WorkerSendRequest {
            responder,
            message: initialize_request,
        } = context.recv_from_handler().await?;
        let _ = responder.send(Ok(()));
        let (message, session_id) = {
            let resp_result = self
                .client
                .post_message(config.uri.clone(), initialize_request, None, None, None)
                .await;
            let resp = match resp_result {
                Err(e) => {
                    return Err(WorkerQuitReason::fatal(e, "send initialize request"));
                }
                Ok(r) => r,
            };
            let expect_result = resp
                .expect_initialized::<C::Error>(self.tmcp_connection.as_ref())
                .await;
            match expect_result {
                Ok((msg, sid)) => (msg, sid),
                Err(e) => {
                    return Err(WorkerQuitReason::fatal(e, "process initialize response"));
                }
            }
        };
        let session_id: Option<Arc<str>> = if let Some(session_id) = session_id {
            Some(session_id.into())
        } else {
            if !self.config.allow_stateless {
                return Err(WorkerQuitReason::HandlerTerminated);
            }
            None
        };
        // delete session when drop guard is dropped
        if let Some(session_id) = &session_id {
            let ct = transport_task_ct.clone();
            let client = self.client.clone();
            let session_id = session_id.clone();
            let url = config.uri.clone();
            tokio::spawn(async move {
                ct.cancelled().await;
                let delete_session_result =
                    client.delete_session(url, session_id.clone(), None).await;
                match delete_session_result {
                    Ok(_) => {
                        tracing::info!(session_id = session_id.as_ref(), "delete session success")
                    }
                    Err(StreamableHttpError::ServerDoesNotSupportDeleteSession) => {
                        tracing::info!(
                            session_id = session_id.as_ref(),
                            "server doesn't support delete session"
                        )
                    }
                    Err(e) => {
                        tracing::error!(
                            session_id = session_id.as_ref(),
                            "fail to delete session: {e}"
                        );
                    }
                };
            });
        }

        context.send_to_handler(message).await?;
        let initialized_notification = context.recv_from_handler().await?;
        // expect a initialized response
        {
            let resp_result = self
                .client
                .post_message(
                    config.uri.clone(),
                    initialized_notification.message,
                    session_id.clone(),
                    None,
                    None,
                )
                .await;
            let resp = match resp_result {
                Err(e) => {
                    return Err(WorkerQuitReason::fatal(e, "send initialized notification"));
                }
                Ok(r) => r,
            };
            match resp.expect_accepted::<C::Error>() {
                Ok(_) => (),
                Err(e) => {
                    return Err(WorkerQuitReason::fatal(
                        e,
                        "process initialized notification response",
                    ));
                }
            }
        }
        let _ = initialized_notification.responder.send(Ok(()));
        enum Event<W: Worker, E: std::error::Error + Send + Sync + 'static> {
            ClientMessage(WorkerSendRequest<W>),
            ServerMessage(ServerJsonRpcMessage),
            StreamResult(Result<(), StreamableHttpError<E>>),
        }
        let mut streams = tokio::task::JoinSet::new();
        if let Some(session_id) = &session_id {
            match self
                .client
                .get_stream(config.uri.clone(), session_id.clone(), None, None)
                .await
            {
                Ok(stream) => {
                    let sse_stream = SseAutoReconnectStream::new(
                        stream,
                        StreamableHttpClientReconnect {
                            client: self.client.clone(),
                            session_id: session_id.clone(),
                            uri: config.uri.clone(),
                        },
                        self.config.retry_config.clone(),
                        self.tmcp_connection.clone(),
                    );
                    streams.spawn(Self::execute_sse_stream(
                        sse_stream,
                        sse_worker_tx.clone(),
                        false,
                        transport_task_ct.child_token(),
                    ));
                    tracing::debug!("got common stream");
                }
                Err(StreamableHttpError::ServerDoesNotSupportSse) => {
                    tracing::debug!("server doesn't support sse, skip common stream");
                }
                Err(e) => {
                    // fail to get common stream
                    tracing::error!("fail to get common stream: {e}");
                    return Err(WorkerQuitReason::fatal(
                        e,
                        "get general purpose event stream",
                    ));
                }
            }
        }
        loop {
            let event = tokio::select! {
                _ = transport_task_ct.cancelled() => {
                    tracing::debug!("cancelled");
                    return Err(WorkerQuitReason::Cancelled);
                }
                message = context.recv_from_handler() => {
                    let message = message?;
                    Event::ClientMessage(message)
                },
                message = sse_worker_rx.recv() => {
                    let Some(message) = message else {
                        tracing::trace!("transport dropped, exiting");
                        return Err(WorkerQuitReason::HandlerTerminated);
                    };
                    Event::ServerMessage(message)
                },
                terminated_stream = streams.join_next(), if !streams.is_empty() => {
                    match terminated_stream {
                        Some(result) => {
                            Event::StreamResult(result.map_err(StreamableHttpError::TokioJoinError).and_then(std::convert::identity))
                        }
                        None => {
                            continue
                        }
                    }
                }
            };
            match event {
                Event::ClientMessage(send_request) => {
                    let WorkerSendRequest { message, responder } = send_request;
                    let response = self
                        .client
                        .post_message(config.uri.clone(), message, session_id.clone(), None, None)
                        .await;
                    let send_result = match response {
                        Err(e) => Err(e),
                        Ok(StreamableHttpPostResponse::Accepted) => {
                            tracing::trace!("client message accepted");
                            Ok(())
                        }
                        Ok(StreamableHttpPostResponse::Json(message, ..)) => {
                            context.send_to_handler(message).await?;
                            Ok(())
                        }
                        Ok(StreamableHttpPostResponse::Sse(stream, ..)) => {
                            if let Some(session_id) = &session_id {
                                let sse_stream = SseAutoReconnectStream::new(
                                    stream,
                                    StreamableHttpClientReconnect {
                                        client: self.client.clone(),
                                        session_id: session_id.clone(),
                                        uri: config.uri.clone(),
                                    },
                                    self.config.retry_config.clone(),
                                    self.tmcp_connection.clone(),
                                );
                                streams.spawn(Self::execute_sse_stream(
                                    sse_stream,
                                    sse_worker_tx.clone(),
                                    true,
                                    transport_task_ct.child_token(),
                                ));
                            } else {
                                let sse_stream = SseAutoReconnectStream::never_reconnect(
                                    stream,
                                    StreamableHttpError::<C::Error>::UnexpectedEndOfStream,
                                );
                                streams.spawn(Self::execute_sse_stream(
                                    sse_stream,
                                    sse_worker_tx.clone(),
                                    true,
                                    transport_task_ct.child_token(),
                                ));
                            }
                            tracing::trace!("got new sse stream");
                            Ok(())
                        }
                    };
                    let _ = responder.send(send_result);
                }
                Event::ServerMessage(json_rpc_message) => {
                    // send the message to the handler
                    context.send_to_handler(json_rpc_message).await?;
                }
                Event::StreamResult(result) => {
                    if result.is_err() {
                        tracing::warn!(
                            "sse client event stream terminated with error: {:?}",
                            result
                        );
                    }
                }
            }
        }
    }
}

pub type StreamableHttpClientTransport<C> = WorkerTransport<StreamableHttpClientWorker<C>>;

impl<C: StreamableHttpClient> StreamableHttpClientTransport<C> {
    pub fn with_client(client: C, config: StreamableHttpClientTransportConfig) -> Self {
        let tmcp_connection = {
            let any_client = &client as &dyn std::any::Any;
            if let Some(tmcp_client) = any_client.downcast_ref::<crate::transport::common::reqwest::streamable_http_client::TmcpReqwestClient>() {
                tmcp_client.tmcp_connection.clone()
            } else {
                None
            }
        };
        let mut worker = StreamableHttpClientWorker::new(client, config);
        worker.tmcp_connection = tmcp_connection;
        WorkerTransport::spawn(worker)
    }
}
#[derive(Debug, Clone)]
pub struct StreamableHttpClientTransportConfig {
    pub uri: Arc<str>,
    pub retry_config: Arc<dyn SseRetryPolicy>,
    pub channel_buffer_capacity: usize,
    /// if true, the transport will not require a session to be established
    pub allow_stateless: bool,
}

impl StreamableHttpClientTransportConfig {
    pub fn with_uri(uri: impl Into<Arc<str>>) -> Self {
        Self {
            uri: uri.into(),
            ..Default::default()
        }
    }
}

impl Default for StreamableHttpClientTransportConfig {
    fn default() -> Self {
        Self {
            uri: "localhost".into(),
            retry_config: Arc::new(ExponentialBackoff::default()),
            channel_buffer_capacity: 16,
            allow_stateless: true,
        }
    }
}

pub fn convert_quit_reason<C: StreamableHttpClient>(
    reason: WorkerQuitReason<StreamableHttpError<C::Error>>,
) -> WorkerQuitReason<C::Error> {
    match reason {
        WorkerQuitReason::Cancelled => WorkerQuitReason::Cancelled,
        WorkerQuitReason::HandlerTerminated => WorkerQuitReason::HandlerTerminated,
        WorkerQuitReason::Join(e) => WorkerQuitReason::Join(e),
        WorkerQuitReason::TransportClosed => WorkerQuitReason::TransportClosed,
        WorkerQuitReason::Fatal { error, context } => match error {
            StreamableHttpError::Client(e) => WorkerQuitReason::Fatal { error: e, context },
            _ => WorkerQuitReason::HandlerTerminated,
        },
    }
}
