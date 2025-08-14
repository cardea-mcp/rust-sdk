pub mod tmcp_client;

#[cfg(feature = "transport-streamable-http-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "transport-streamable-http-client")))]
pub mod streamable_http_client;

#[cfg(feature = "transport-sse-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "transport-sse-client")))]
pub mod sse_client;
