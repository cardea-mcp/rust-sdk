use std::sync::Arc;

use futures::StreamExt;
use http::Uri;
use reqwest::header::ACCEPT;
use sse_stream::SseStream;

pub use crate::transport::common::reqwest::tmcp_client::TmcpReqwestClient;
use crate::{
    model::ClientJsonRpcMessage,
    transport::{
        SseClientTransport,
        common::http_header::{EVENT_STREAM_MIME_TYPE, HEADER_LAST_EVENT_ID},
        sse_client::{SseClient, SseClientConfig, SseTransportError},
    },
};
pub type TmcpSseReqwestClient = TmcpReqwestClient;
use crate::transport::common::TmcpMessageCodec;

impl SseClient for TmcpSseReqwestClient {
    type Error = reqwest::Error;

    async fn post_message(
        &self,
        uri: Uri,
        message: ClientJsonRpcMessage,
        auth_token: Option<String>,
    ) -> Result<(), SseTransportError<Self::Error>> {
        let mut request_builder = self.client.post(uri.to_string());
        if self.tmcp_connection.is_some() {
            let sealed = self
                .seal_message(&serde_json::to_string(&message).unwrap())
                .unwrap_or_else(|_| serde_json::to_string(&message).unwrap());
            request_builder = request_builder
                .body(sealed)
                .header("content-type", "application/tsp");
        } else {
            request_builder = request_builder.json(&message);
        }
        if let Some(auth_header) = auth_token {
            request_builder = request_builder.bearer_auth(auth_header);
        }
        request_builder
            .send()
            .await
            .and_then(|resp| resp.error_for_status())
            .map_err(SseTransportError::from)
            .map(drop)
    }

    async fn get_stream(
        &self,
        uri: Uri,
        last_event_id: Option<String>,
        auth_token: Option<String>,
    ) -> Result<
        crate::transport::common::client_side_sse::BoxedSseResponse,
        SseTransportError<Self::Error>,
    > {
        let mut request_builder = self
            .client
            .get(uri.to_string())
            .header(ACCEPT, EVENT_STREAM_MIME_TYPE);
        if let Some(auth_header) = auth_token {
            request_builder = request_builder.bearer_auth(auth_header);
        }
        if let Some(last_event_id) = last_event_id {
            request_builder = request_builder.header(HEADER_LAST_EVENT_ID, last_event_id);
        }
        let response = request_builder.send().await?;
        let response = response.error_for_status()?;
        match response.headers().get(reqwest::header::CONTENT_TYPE) {
            Some(ct) => {
                if !ct.as_bytes().starts_with(EVENT_STREAM_MIME_TYPE.as_bytes()) {
                    return Err(SseTransportError::UnexpectedContentType(Some(ct.clone())));
                }
            }
            None => {
                return Err(SseTransportError::UnexpectedContentType(None));
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
                                    let client = TmcpSseReqwestClient {
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
                        Err(e) => Some(Err(sse_stream::Error::from(e))),
                    }
                }
            })
            .boxed();
        Ok(event_stream)
    }
}

impl SseClientTransport<TmcpSseReqwestClient> {
    pub async fn start_with_tmcp(
        uri: impl Into<Arc<str>>,
        tmcp_connection: Option<crate::transport::common::tmcp::TmcpConnection>,
    ) -> Result<Self, SseTransportError<reqwest::Error>> {
        SseClientTransport::start_with_client(
            TmcpSseReqwestClient {
                client: reqwest::Client::default(),
                tmcp_connection,
            },
            SseClientConfig {
                sse_endpoint: uri.into(),
                ..Default::default()
            },
        )
        .await
    }
}
