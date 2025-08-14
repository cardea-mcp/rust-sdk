#[derive(Clone)]
pub struct TmcpReqwestClient {
    pub client: reqwest::Client,
    pub tmcp_connection: Option<crate::transport::common::tmcp::TmcpConnection>,
}
