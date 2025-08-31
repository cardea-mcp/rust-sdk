use std::{convert::Infallible, sync::Arc};

use bytes::Bytes;
use http::request::Parts;
use http_body::Body;
use http_body_util::{BodyExt, Full, combinators::BoxBody};

use crate::{
    model::ClientJsonRpcMessage,
    transport::common::{
        server_side_http::expect_json,
        tmcp::{TmcpConnection, TmcpIdentityManager},
    },
};

pub fn get_client_did(part: &http::request::Parts) -> String {
    [
        // headers: "did"
        part.headers
            .get("did")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        // query string
        part.uri.query().and_then(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .find(|(k, _)| k == "did")
                .map(|(_, v)| v.into_owned())
        }),
        // referer header
        part.headers
            .get("referer")
            .and_then(|v| v.to_str().ok())
            .and_then(|referer_str| {
                referer_str.find("did=").map(|idx| {
                    let did_part = &referer_str[idx + 4..];
                    did_part.split('&').next().unwrap_or(did_part).to_string()
                })
            }),
    ]
    .into_iter()
    .flatten()
    .find(|did| !did.is_empty())
    .unwrap_or_default()
}

pub fn serialize_event(event_type: &str, data: &str) -> String {
    serde_json::json!({
        "event": event_type,
        "data": data
    })
    .to_string()
}

pub fn parse_event(data: &str) -> Option<(String, String)> {
    match serde_json::from_str::<serde_json::Value>(data) {
        Ok(json) => {
            let event_type = json
                .get("event")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let event_data = json
                .get("data")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            match (event_type, event_data) {
                (Some(e), Some(d)) => Some((e, d)),
                _ => None,
            }
        }
        Err(_) => None,
    }
}

pub async fn ensure_tmcp_connection(
    manager: &Arc<TmcpIdentityManager>,
    did: &str,
) -> Result<TmcpConnection, http::Response<BoxBody<Bytes, Infallible>>> {
    let did_decoded = percent_encoding::percent_decode_str(did)
        .decode_utf8_lossy()
        .to_string();
    match manager.get_connection(&did_decoded).await {
        Ok(conn) => Ok(conn),
        Err(_) => Err(http::Response::builder()
            .status(http::StatusCode::BAD_REQUEST)
            .body(BoxBody::new(Full::new(Bytes::from(
                "TMCP connection error",
            ))))
            .expect("valid response")),
    }
}

pub async fn parse_message_by_content_type<B>(
    content_type: &str,
    body: B,
    manager: Option<Arc<TmcpIdentityManager>>,
    part: &Parts,
) -> Result<ClientJsonRpcMessage, http::Response<BoxBody<Bytes, Infallible>>>
where
    B: Body + Send + 'static,
    B::Error: std::fmt::Display,
{
    use base64::Engine;

    let ct = content_type
        .trim()
        .split(';')
        .next()
        .unwrap_or(content_type);

    let bytes = match body.collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => {
            return Err(http::Response::builder()
                .status(http::StatusCode::BAD_REQUEST)
                .body(BoxBody::new(Full::new(Bytes::from("failed to read body"))))
                .expect("valid response"));
        }
    };
    let raw = String::from_utf8_lossy(&bytes);

    if (ct == "application/json" || ct == "application/tsp")
        && (raw.starts_with("-EABX")
            || raw
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '='))
    {
        let manager = match manager {
            Some(m) => m,
            None => {
                return Err(http::Response::builder()
                    .status(http::StatusCode::BAD_REQUEST)
                    .body(BoxBody::new(Full::new(Bytes::from("No TMCP manager"))))
                    .expect("valid response"));
            }
        };
        let did = {
            let from_part = get_client_did(part);
            if !from_part.is_empty() {
                from_part
            } else {
                manager.get_did().to_string()
            }
        };
        if did.is_empty() {
            return Err(http::Response::builder()
                .status(http::StatusCode::BAD_REQUEST)
                .body(BoxBody::new(Full::new(Bytes::from("Manager DID empty"))))
                .expect("valid response"));
        }
        let decoded_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw.as_bytes())
            .unwrap_or_else(|_| bytes.clone().to_vec());
        let tmcp = match ensure_tmcp_connection(&manager, &did).await {
            Ok(conn) => conn,
            Err(resp) => return Err(resp),
        };
        let (sender_did, receiver_did) = match manager.get_sender_receiver(&decoded_bytes) {
            Ok((s, r)) => (s, r),
            Err(_) => (get_client_did(part), get_client_did(part)),
        };
        // silent fallback: allow initialization to continue
        let decoded = match tmcp.open_message(&raw) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(
                    "TMCP open_message error: {:?}, sender_did: {}, receiver_did: {}, raw: {}",
                    e,
                    sender_did,
                    receiver_did,
                    raw
                );
                return Err(http::Response::builder()
                    .status(http::StatusCode::BAD_REQUEST)
                    .body(BoxBody::new(Full::new(Bytes::from(format!(
                        "TMCP decode error: {:?}",
                        e
                    )))))
                    .expect("valid response"));
            }
        };
        return match serde_json::from_str(&decoded) {
            Ok(message) => Ok(message),
            Err(_) => Err(http::Response::builder()
                .status(http::StatusCode::BAD_REQUEST)
                .body(BoxBody::new(Full::new(Bytes::from("Invalid TMCP JSON"))))
                .expect("valid response")),
        };
    }

    match ct {
        "application/json" => match serde_json::from_slice(&bytes) {
            Ok(message) => Ok(message),
            Err(_) => Err(http::Response::builder()
                .status(http::StatusCode::BAD_REQUEST)
                .body(BoxBody::new(Full::new(Bytes::from(
                    "Invalid application/json",
                ))))
                .expect("valid response")),
        },
        "text/plain" => match serde_json::from_str(&raw) {
            Ok(message) => Ok(message),
            Err(_) => Err(http::Response::builder()
                .status(http::StatusCode::BAD_REQUEST)
                .body(BoxBody::new(Full::new(Bytes::from(
                    "Invalid text/plain JSON",
                ))))
                .expect("valid response")),
        },
        _ => match expect_json(BoxBody::new(Full::new(bytes))).await {
            Ok(message) => Ok(message),
            Err(response) => {
                let (parts, body) = response.into_parts();
                Err(http::Response::from_parts(parts, BoxBody::new(body)))
            }
        },
    }
}
