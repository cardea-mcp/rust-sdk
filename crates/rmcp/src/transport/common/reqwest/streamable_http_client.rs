use std::sync::Arc;

use futures::{StreamExt, stream::BoxStream};
use reqwest::header::ACCEPT;
use sse_stream::{Sse, SseStream};

pub use crate::transport::common::reqwest::tmcp_client::TmcpReqwestClient;
use crate::{
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::{
        common::{
            TmcpMessageCodec,
            http_header::{
                EVENT_STREAM_MIME_TYPE, HEADER_LAST_EVENT_ID, HEADER_SESSION_ID, JSON_MIME_TYPE,
            },
        },
        streamable_http_client::*,
    },
};

impl StreamableHttpClient for TmcpReqwestClient {
    type Error = reqwest::Error;

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        last_event_id: Option<String>,
        auth_token: Option<String>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<reqwest::Error>>
    {
        let mut request_builder = self
            .client
            .get(uri.as_ref())
            .header(ACCEPT, EVENT_STREAM_MIME_TYPE)
            .header(HEADER_SESSION_ID, session_id.as_ref());
        if let Some(last_event_id) = last_event_id {
            request_builder = request_builder.header(HEADER_LAST_EVENT_ID, last_event_id);
        }
        if let Some(auth_header) = auth_token {
            request_builder = request_builder.bearer_auth(auth_header);
        }
        let response = request_builder
            .send()
            .await
            .map_err(StreamableHttpError::Client)?;
        if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            return Err(StreamableHttpError::ServerDoesNotSupportSse);
        }
        let response = response
            .error_for_status()
            .map_err(StreamableHttpError::Client)?;
        match response.headers().get(reqwest::header::CONTENT_TYPE) {
            Some(ct) => {
                if !ct.as_bytes().starts_with(EVENT_STREAM_MIME_TYPE.as_bytes()) {
                    return Err(StreamableHttpError::UnexpectedContentType(Some(
                        String::from_utf8_lossy(ct.as_bytes()).to_string(),
                    )));
                }
            }
            None => {
                return Err(StreamableHttpError::UnexpectedContentType(None));
            }
        }
        let tmcp_connection = self.tmcp_connection.clone();
        let event_stream = SseStream::from_byte_stream(response.bytes_stream())
            .filter_map(move |evt| {
                let tmcp_connection = tmcp_connection.clone();
                async move {
                    match evt {
                        Ok(mut sse) => {
                            if let Some(ref tmcp_conn) = tmcp_connection {
                                if let Some(data) = &sse.data {
                                    let client = TmcpReqwestClient {
                                        client: reqwest::Client::default(),
                                        tmcp_connection: Some(tmcp_conn.clone()),
                                    };
                                    match client.open_message(data) {
                                        Ok(opened) => {
                                            sse.data = Some(opened);
                                            Some(Ok(sse))
                                        }
                                        Err(_) => Some(Ok(sse)),
                                    }
                                } else {
                                    Some(Ok(sse))
                                }
                            } else {
                                Some(Ok(sse))
                            }
                        }
                        Err(e) => Some(Err(SseError::from(e))),
                    }
                }
            })
            .boxed();
        Ok(event_stream)
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        auth_token: Option<String>,
    ) -> Result<(), StreamableHttpError<reqwest::Error>> {
        let mut request_builder = self.client.delete(uri.as_ref());
        if let Some(auth_header) = auth_token {
            request_builder = request_builder.bearer_auth(auth_header);
        }
        let response = request_builder
            .header(HEADER_SESSION_ID, session.as_ref())
            .send()
            .await
            .map_err(StreamableHttpError::Client)?;

        // if method no allowed
        if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            tracing::debug!("this server doesn't support deleting session");
            return Ok(());
        }
        let _response = response
            .error_for_status()
            .map_err(StreamableHttpError::Client)?;
        Ok(())
    }

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_token: Option<String>,
        content: Option<String>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<reqwest::Error>> {
        let mut request = self
            .client
            .post(uri.as_ref())
            .header(ACCEPT, [EVENT_STREAM_MIME_TYPE, JSON_MIME_TYPE].join(", "));
        if let Some(auth_header) = auth_token {
            request = request.bearer_auth(auth_header);
        }
        if let Some(session_id) = session_id {
            request = request.header(HEADER_SESSION_ID, session_id.as_ref());
        }
        let response = if content.is_some() || self.tmcp_connection.is_some() {
            let sealed = self
                .seal_message(&serde_json::to_string(&message).unwrap())
                .unwrap_or_else(|_| serde_json::to_string(&message).unwrap());
            request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(sealed)
                .send()
                .await
                .map_err(StreamableHttpError::Client)?
                .error_for_status()
                .map_err(StreamableHttpError::Client)?
        } else {
            request
                .json(&message)
                .send()
                .await
                .map_err(StreamableHttpError::Client)?
                .error_for_status()
                .map_err(StreamableHttpError::Client)?
        };
        if response.status() == reqwest::StatusCode::ACCEPTED {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        let content_type = response.headers().get(reqwest::header::CONTENT_TYPE);
        let session_id = response.headers().get(HEADER_SESSION_ID);
        let session_id = session_id
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        match content_type {
            Some(ct) if ct.as_bytes().starts_with(EVENT_STREAM_MIME_TYPE.as_bytes()) => {
                let event_stream = SseStream::from_byte_stream(response.bytes_stream()).boxed();
                Ok(StreamableHttpPostResponse::Sse(event_stream, session_id))
            }
            Some(ct) if ct.as_bytes().starts_with(JSON_MIME_TYPE.as_bytes()) => {
                let message: ServerJsonRpcMessage =
                    response.json().await.map_err(StreamableHttpError::Client)?;
                Ok(StreamableHttpPostResponse::Json(message, session_id))
            }
            _ => {
                // unexpected content type
                tracing::error!("unexpected content type: {:?}", content_type);
                Err(StreamableHttpError::UnexpectedContentType(
                    content_type.map(|ct| String::from_utf8_lossy(ct.as_bytes()).to_string()),
                ))
            }
        }
    }
}

impl StreamableHttpClientTransport<TmcpReqwestClient> {
    pub fn from_uri(uri: impl Into<Arc<str>>) -> Self {
        StreamableHttpClientTransport::with_client(
            TmcpReqwestClient {
                client: reqwest::Client::default(),
                tmcp_connection: None,
            },
            StreamableHttpClientTransportConfig {
                uri: uri.into(),
                ..Default::default()
            },
        )
    }
    pub fn from_uri_with_tmcp(
        uri: impl Into<Arc<str>>,
        tmcp_connection: Option<crate::transport::common::tmcp::TmcpConnection>,
    ) -> Self {
        StreamableHttpClientTransport::with_client(
            TmcpReqwestClient {
                client: reqwest::Client::default(),
                tmcp_connection,
            },
            StreamableHttpClientTransportConfig {
                uri: uri.into(),
                ..Default::default()
            },
        )
    }
}
