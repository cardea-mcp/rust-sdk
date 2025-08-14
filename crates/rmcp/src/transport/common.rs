#[cfg(any(
    feature = "transport-streamable-http-server",
    feature = "transport-sse-server"
))]
pub mod server_side_http;

pub mod http_header;

#[cfg(feature = "__reqwest")]
#[cfg_attr(docsrs, doc(cfg(feature = "reqwest")))]
pub mod reqwest;

#[cfg(feature = "client-side-sse")]
#[cfg_attr(docsrs, doc(cfg(feature = "client-side-sse")))]
pub mod client_side_sse;

#[cfg(feature = "auth")]
#[cfg_attr(docsrs, doc(cfg(feature = "auth")))]
pub mod auth;

pub mod tmcp;

pub trait TmcpMessageCodec {
    fn seal_message(&self, message: &str) -> anyhow::Result<String>;
    fn open_message(&self, encoded: &str) -> anyhow::Result<String>;
}

impl TmcpMessageCodec for crate::transport::common::tmcp::TmcpConnection {
    fn seal_message(&self, message: &str) -> anyhow::Result<String> {
        self.seal_message(message)
    }
    fn open_message(&self, encoded: &str) -> anyhow::Result<String> {
        self.open_message(encoded)
    }
}
