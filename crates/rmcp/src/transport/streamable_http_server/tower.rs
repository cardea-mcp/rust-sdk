use std::{convert::Infallible, fmt::Display, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::{StreamExt, future::BoxFuture};
use http::{Method, Request, Response, header::ALLOW};
use http_body::Body;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use tokio_stream::wrappers::ReceiverStream;

use super::session::SessionManager;
use crate::{
    RoleServer,
    model::{ClientJsonRpcMessage, ClientRequest, GetExtensions},
    serve_server,
    service::serve_directly,
    transport::{
        OneshotTransport, TransportAdapterIdentity,
        common::{
            http_header::{
                EVENT_STREAM_MIME_TYPE, HEADER_LAST_EVENT_ID, HEADER_SESSION_ID, JSON_MIME_TYPE,
            },
            server_side_http::{
                BoxResponse, ServerSseMessage, accepted_response, internal_error_response,
                sse_stream_response, unexpected_message_response,
            },
        },
        tsp_utils::get_client_did,
    },
};

#[derive(Debug, Clone)]
pub struct StreamableHttpServerConfig {
    /// The ping message duration for SSE connections.
    pub sse_keep_alive: Option<Duration>,
    /// If true, the server will create a session for each request and keep it alive.
    pub stateful_mode: bool,
    /// Optional TMCP identity manager for DID trust.
    pub manager: Option<Arc<crate::transport::common::tmcp::TmcpIdentityManager>>,
}

impl Default for StreamableHttpServerConfig {
    fn default() -> Self {
        Self {
            sse_keep_alive: Some(Duration::from_secs(15)),
            stateful_mode: true,
            manager: None,
        }
    }
}

/// # Streamable Http Server
///
/// ## Extract information from raw http request
///
/// The http service will consume the request body, however the rest part will be remain and injected into [`crate::model::Extensions`],
/// which you can get from [`crate::service::RequestContext`].
/// ```rust
/// use rmcp::handler::server::tool::Extension;
/// use http::request::Parts;
/// async fn my_tool(Extension(parts): Extension<Parts>) {
///     tracing::info!("http parts:{parts:?}")
/// }
/// ```
pub struct StreamableHttpService<S, M = super::session::local::LocalSessionManager> {
    pub config: StreamableHttpServerConfig,
    session_manager: Arc<M>,
    service_factory: Arc<dyn Fn() -> Result<S, std::io::Error> + Send + Sync>,
}

impl<S, M> Clone for StreamableHttpService<S, M> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            session_manager: self.session_manager.clone(),
            service_factory: self.service_factory.clone(),
        }
    }
}

impl<RequestBody, S, M> tower_service::Service<Request<RequestBody>> for StreamableHttpService<S, M>
where
    RequestBody: Body + Send + 'static,
    S: crate::Service<RoleServer>,
    M: SessionManager,
    RequestBody::Error: Display,
    RequestBody::Data: Send + 'static,
{
    type Response = BoxResponse;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn call(&mut self, req: http::Request<RequestBody>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            let response = service.handle(req).await;
            Ok(response)
        })
    }
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

impl<S, M> StreamableHttpService<S, M>
where
    S: crate::Service<RoleServer> + Send + 'static,
    M: SessionManager,
{
    pub fn new(
        service_factory: impl Fn() -> Result<S, std::io::Error> + Send + Sync + 'static,
        session_manager: Arc<M>,
        config: StreamableHttpServerConfig,
    ) -> Self {
        Self {
            config,
            session_manager,
            service_factory: Arc::new(service_factory),
        }
    }
    fn get_service(&self) -> Result<S, std::io::Error> {
        (self.service_factory)()
    }
    pub async fn handle<B>(&self, request: Request<B>) -> Response<BoxBody<Bytes, Infallible>>
    where
        B: Body + Send + 'static,
        B::Error: Display,
    {
        let method = request.method().clone();
        let allowed_methods = match self.config.stateful_mode {
            true => "GET, POST, DELETE",
            false => "POST",
        };
        let result = match (method, self.config.stateful_mode) {
            (Method::POST, _) => self.handle_post(request).await,
            // if we're not in stateful mode, we don't support GET or DELETE because there is no session
            (Method::GET, true) => self.handle_get(request).await,
            (Method::DELETE, true) => self.handle_delete(request).await,
            _ => {
                // Handle other methods or return an error
                let response = Response::builder()
                    .status(http::StatusCode::METHOD_NOT_ALLOWED)
                    .header(ALLOW, allowed_methods)
                    .body(Full::new(Bytes::from("Method Not Allowed")).boxed())
                    .expect("valid response");
                return response;
            }
        };
        match result {
            Ok(response) => response,
            Err(response) => response,
        }
    }
    async fn handle_get<B>(&self, request: Request<B>) -> Result<BoxResponse, BoxResponse>
    where
        B: Body + Send + 'static,
        B::Error: Display,
    {
        if !request
            .headers()
            .get(http::header::ACCEPT)
            .and_then(|header| header.to_str().ok())
            .is_some_and(|header| header.contains(EVENT_STREAM_MIME_TYPE))
        {
            return Ok(Response::builder()
                .status(http::StatusCode::NOT_ACCEPTABLE)
                .body(
                    Full::new(Bytes::from(
                        "Not Acceptable: Client must accept text/event-stream",
                    ))
                    .boxed(),
                )
                .expect("valid response"));
        }
        let session_id = request
            .headers()
            .get(HEADER_SESSION_ID)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned().into());
        let Some(session_id) = session_id else {
            return Ok(Response::builder()
                .status(http::StatusCode::UNAUTHORIZED)
                .body(Full::new(Bytes::from("Unauthorized: Session ID is required")).boxed())
                .expect("valid response"));
        };
        // check if session exists
        let has_session = self
            .session_manager
            .has_session(&session_id)
            .await
            .map_err(internal_error_response("check session"))?;
        if !has_session {
            // unauthorized
            return Ok(Response::builder()
                .status(http::StatusCode::UNAUTHORIZED)
                .body(Full::new(Bytes::from("Unauthorized: Session not found")).boxed())
                .expect("valid response"));
        }
        // check if last event id is provided
        let last_event_id = request
            .headers()
            .get(HEADER_LAST_EVENT_ID)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());
        if let Some(last_event_id) = last_event_id {
            // check if session has this event id
            let stream = self
                .session_manager
                .resume(&session_id, last_event_id)
                .await
                .map_err(internal_error_response("resume session"))?;
            Ok(sse_stream_response(stream, self.config.sse_keep_alive))
        } else {
            // create standalone stream
            let stream = self
                .session_manager
                .create_standalone_stream(&session_id)
                .await
                .map_err(internal_error_response("create standalone stream"))?;
            Ok(sse_stream_response(stream, self.config.sse_keep_alive))
        }
    }

    async fn handle_post<B>(&self, request: Request<B>) -> Result<BoxResponse, BoxResponse>
    where
        B: Body + Send + 'static,
        B::Error: Display,
    {
        // check accept header
        if !request
            .headers()
            .get(http::header::ACCEPT)
            .and_then(|header| header.to_str().ok())
            .is_some_and(|header| {
                header.contains(JSON_MIME_TYPE) && header.contains(EVENT_STREAM_MIME_TYPE)
            })
        {
            return Ok(Response::builder()
                .status(http::StatusCode::NOT_ACCEPTABLE)
                .body(Full::new(Bytes::from("Not Acceptable: Client must accept both application/json and text/event-stream")).boxed())
                .expect("valid response"));
        }

        // json deserialize request body
        let (part, body) = request.into_parts();
        let content_type = part
            .headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|header| header.to_str().ok())
            .unwrap_or("<none>");
        if !["application/json", "text/plain", "application/octet-stream"]
            .iter()
            .any(|ct| content_type.starts_with(ct))
        {
            return Ok(Response::builder()
                .status(http::StatusCode::UNSUPPORTED_MEDIA_TYPE)
                .body(
                    Full::new(Bytes::from(
                        format!("Unsupported Media Media: Content-Type was '{}', must be application/json, text/plain, or application/octet-stream", content_type),
                    ))
                    .boxed(),
                )
                .expect("valid response"));
        }
        let mut message = match crate::transport::tsp_utils::parse_message_by_content_type(
            content_type,
            body,
            self.config.manager.clone(),
            &part,
        )
        .await
        {
            Ok(msg) => msg,
            Err(response) => {
                return Ok(response);
            }
        };

        if self.config.stateful_mode {
            // do we have a session id?
            let session_id = part
                .headers
                .get(HEADER_SESSION_ID)
                .and_then(|v| v.to_str().ok());
            if let Some(session_id) = session_id {
                let session_id = session_id.to_owned().into();
                let has_session = self
                    .session_manager
                    .has_session(&session_id)
                    .await
                    .map_err(internal_error_response("check session"))?;
                if !has_session {
                    // unauthorized
                    return Ok(Response::builder()
                        .status(http::StatusCode::UNAUTHORIZED)
                        .body(Full::new(Bytes::from("Unauthorized: Session not found")).boxed())
                        .expect("valid response"));
                }

                // inject request part to extensions
                match &mut message {
                    ClientJsonRpcMessage::Request(req) => {
                        req.request.extensions_mut().insert(part.clone());
                    }
                    ClientJsonRpcMessage::Notification(not) => {
                        not.notification.extensions_mut().insert(part.clone());
                    }
                    _ => {
                        // skip
                    }
                }

                match message {
                    ClientJsonRpcMessage::Request(_) => {
                        let client_did = get_client_did(&part);
                        let tmcp = if let Some(manager) = self.config.manager.clone() {
                            manager.get_connection(&client_did).await.ok()
                        } else {
                            None
                        };
                        let stream = self
                            .session_manager
                            .create_stream(&session_id, message)
                            .await
                            .map_err(internal_error_response("get session"))?
                            .map(move |msg| {
                                let msg_str = msg.message.as_ref();
                                if let Some(tmcp) = &tmcp {
                                    let sealed =
                                        tmcp.seal_message(msg_str).unwrap_or(msg_str.to_string());
                                    ServerSseMessage {
                                        event_id: msg.event_id.clone(),
                                        message: Arc::new(sealed),
                                    }
                                } else {
                                    ServerSseMessage {
                                        event_id: msg.event_id.clone(),
                                        message: Arc::new(msg_str.to_string()),
                                    }
                                }
                            });
                        Ok(sse_stream_response(stream, self.config.sse_keep_alive))
                    }
                    ClientJsonRpcMessage::Notification(_)
                    | ClientJsonRpcMessage::Response(_)
                    | ClientJsonRpcMessage::Error(_) => {
                        // handle notification
                        self.session_manager
                            .accept_message(&session_id, message)
                            .await
                            .map_err(internal_error_response("accept message"))?;
                        Ok(accepted_response())
                    }
                    _ => Ok(Response::builder()
                        .status(http::StatusCode::NOT_IMPLEMENTED)
                        .body(
                            Full::new(Bytes::from("Batch requests are not supported yet")).boxed(),
                        )
                        .expect("valid response")),
                }
            } else {
                let (session_id, transport) = self
                    .session_manager
                    .create_session()
                    .await
                    .map_err(internal_error_response("create session"))?;
                let client_did = get_client_did(&part);
                let mut _tmcp_connection = None;
                if let (Some(manager), false) = (&self.config.manager, client_did.is_empty()) {
                    if let Ok(conn) = manager.get_connection(&client_did).await {
                        _tmcp_connection = Some(conn);
                    }
                }
                if let ClientJsonRpcMessage::Request(req) = &mut message {
                    if !matches!(req.request, ClientRequest::InitializeRequest(_)) {
                        return Err(unexpected_message_response("initialize request"));
                    }
                    // inject request part to extensions
                    req.request.extensions_mut().insert(part.clone());
                } else {
                    return Err(unexpected_message_response("initialize request"));
                }
                let service = self
                    .get_service()
                    .map_err(internal_error_response("get service"))?;
                // spawn a task to serve the session
                tokio::spawn({
                    let session_manager = self.session_manager.clone();
                    let session_id = session_id.clone();
                    async move {
                        let service = serve_server::<S, M::Transport, _, TransportAdapterIdentity>(
                            service, transport,
                        )
                        .await;
                        match service {
                            Ok(service) => {
                                // on service created
                                let _ = service.waiting().await;
                            }
                            Err(e) => {
                                tracing::error!("Failed to create service: {e}");
                            }
                        }
                        let _ = session_manager
                            .close_session(&session_id)
                            .await
                            .inspect_err(|e| {
                                tracing::error!("Failed to close session {session_id}: {e}");
                            });
                    }
                });
                // get initialize response
                let response = self
                    .session_manager
                    .initialize_session(&session_id, message)
                    .await
                    .map_err(internal_error_response("create stream"))?;
                let keep_alive = self.config.sse_keep_alive;
                let mut response = if let Some(manager) = &self.config.manager {
                    let client_did = get_client_did(&part);
                    match manager.get_connection(&client_did).await {
                        Ok(tmcp) => sse_stream_response(
                            futures::stream::once({
                                let tmcp = tmcp.clone();
                                async move {
                                    let msg_json = serde_json::to_string(&response).unwrap();
                                    let sealed = tmcp.seal_message(&msg_json).unwrap_or(msg_json);
                                    ServerSseMessage {
                                        event_id: None,
                                        message: Arc::new(sealed),
                                    }
                                }
                            }),
                            keep_alive,
                        ),
                        Err(_) => sse_stream_response(
                            futures::stream::once({
                                async move {
                                    let msg_json = serde_json::to_string(&response).unwrap();
                                    ServerSseMessage {
                                        event_id: None,
                                        message: Arc::new(msg_json),
                                    }
                                }
                            }),
                            keep_alive,
                        ),
                    }
                } else {
                    sse_stream_response(
                        futures::stream::once({
                            async move {
                                let msg_json = serde_json::to_string(&response).unwrap();
                                ServerSseMessage {
                                    event_id: None,
                                    message: Arc::new(msg_json),
                                }
                            }
                        }),
                        keep_alive,
                    )
                };

                response.headers_mut().insert(
                    HEADER_SESSION_ID,
                    session_id
                        .parse()
                        .map_err(internal_error_response("create session id header"))?,
                );
                Ok(response)
            }
        } else {
            let service = self
                .get_service()
                .map_err(internal_error_response("get service"))?;
            match message {
                ClientJsonRpcMessage::Request(mut request) => {
                    request.request.extensions_mut().insert(part);
                    let (transport, receiver) =
                        OneshotTransport::<RoleServer>::new(ClientJsonRpcMessage::Request(request));
                    let service = serve_directly(service, transport, None);
                    tokio::spawn(async move {
                        // on service created
                        let _ = service.waiting().await;
                    });
                    let tmcp = if let Some(manager) = self.config.manager.clone() {
                        manager.get_connection("").await.ok()
                    } else {
                        None
                    };
                    Ok(sse_stream_response(
                        ReceiverStream::new(receiver).map(move |message| {
                            tracing::info!(?message);
                            let msg_json = serde_json::to_string(&message).unwrap();
                            if let Some(tmcp) = &tmcp {
                                let sealed =
                                    tmcp.seal_message(&msg_json).unwrap_or(msg_json.clone());
                                ServerSseMessage {
                                    event_id: None,
                                    message: Arc::new(sealed),
                                }
                            } else {
                                ServerSseMessage {
                                    event_id: None,
                                    message: Arc::new(msg_json),
                                }
                            }
                        }),
                        self.config.sse_keep_alive,
                    ))
                }
                ClientJsonRpcMessage::Notification(_notification) => {
                    // ignore
                    if let Some(manager) = &self.config.manager {
                        let resp = accepted_response();
                        let body_str = "Accepted";
                        let sealed = match manager.get_connection("").await {
                            Ok(tmcp) => tmcp.seal_message(body_str).unwrap_or(body_str.to_string()),
                            Err(_) => body_str.to_string(),
                        };
                        Ok(Response::builder()
                            .status(resp.status())
                            .body(Full::new(Bytes::from(sealed)).boxed())
                            .expect("valid response"))
                    } else {
                        Ok(accepted_response())
                    }
                }
                ClientJsonRpcMessage::Response(_json_rpc_response) => Ok(accepted_response()),
                ClientJsonRpcMessage::Error(_json_rpc_error) => Ok(accepted_response()),
                _ => Ok(Response::builder()
                    .status(http::StatusCode::NOT_IMPLEMENTED)
                    .body(Full::new(Bytes::from("Batch requests are not supported yet")).boxed())
                    .expect("valid response")),
            }
        }
    }

    async fn handle_delete<B>(&self, request: Request<B>) -> Result<BoxResponse, BoxResponse>
    where
        B: Body + Send + 'static,
        B::Error: Display,
    {
        // check session id
        let session_id = request
            .headers()
            .get(HEADER_SESSION_ID)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned().into());
        let Some(session_id) = session_id else {
            // unauthorized
            return Ok(Response::builder()
                .status(http::StatusCode::UNAUTHORIZED)
                .body(Full::new(Bytes::from("Unauthorized: Session ID is required")).boxed())
                .expect("valid response"));
        };
        // close session
        self.session_manager
            .close_session(&session_id)
            .await
            .map_err(internal_error_response("close session"))?;
        Ok(accepted_response())
    }
}
