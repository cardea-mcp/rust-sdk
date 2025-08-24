pub struct TmcpReqwestClient {
    pub client: reqwest::Client,
    pub tmcp_connection: Option<crate::transport::common::tmcp::TmcpConnection>,
}

impl crate::transport::common::TmcpMessageCodec for TmcpReqwestClient {
    fn seal_message(&self, message: &str) -> anyhow::Result<String> {
        match &self.tmcp_connection {
            Some(conn) => conn.seal_message(message),
            None => Ok(message.to_string()),
        }
    }
    fn open_message(&self, encoded: &str) -> anyhow::Result<String> {
        match &self.tmcp_connection {
            Some(conn) => conn.open_message(encoded),
            None => Ok(encoded.to_string()),
        }
    }
}

impl Clone for TmcpReqwestClient {
    fn clone(&self) -> Self {
        TmcpReqwestClient {
            client: self.client.clone(),
            tmcp_connection: self.tmcp_connection.clone(),
        }
    }
}
