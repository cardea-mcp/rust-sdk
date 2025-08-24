use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use tsp_sdk::{OwnedVid, ReceivedTspMessage, SecureStore, VerifiedVid, vid::verify_vid};
use url::Url;
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
pub struct TmcpSettings {
    pub did_publish_url: String,
    pub did_publish_history_url: String,
    pub did_web_format: String,
    pub did_webvh_format: String,
    pub transport: String,
    pub wallet_url: String,
    pub wallet_password: String,
    pub use_webvh: bool,
}

impl Default for TmcpSettings {
    fn default() -> Self {
        Self {
            did_publish_url: "https://did.teaspoon.world/add-vid".into(),
            did_publish_history_url: "https://did.teaspoon.world/add-history/{did}".into(),
            did_web_format: "did:web:did.teaspoon.world:endpoint:{name}".into(),
            did_webvh_format: "did.teaspoon.world/endpoint/{name}".into(),
            transport: "tmcpclient://".into(),
            wallet_url: "sqlite://wallet.sqlite".into(),
            wallet_password: "unsecure".into(),
            use_webvh: true,
        }
    }
}

use std::sync::{Arc, RwLock};

pub struct TmcpIdentityManager {
    settings: TmcpSettings,
    wallet: Arc<RwLock<SecureStore>>,
    pub did: String,
}

impl std::fmt::Debug for TmcpIdentityManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TmcpIdentityManager")
            .field("did", &self.did)
            .finish()
    }
}

impl Clone for TmcpIdentityManager {
    fn clone(&self) -> Self {
        Self {
            settings: self.settings.clone(),
            wallet: self.wallet.clone(),
            did: self.did.clone(),
        }
    }
}

impl TmcpIdentityManager {
    pub fn get_did(&self) -> &str {
        &self.did
    }

    pub fn add_verified_vid(&self, vid: tsp_sdk::Vid) -> anyhow::Result<()> {
        self.wallet.write().unwrap().add_verified_vid(vid, None)?;
        Ok(())
    }

    pub fn get_sender_receiver(&self, msg: &[u8]) -> anyhow::Result<(String, String)> {
        let mut binding = msg.to_vec();
        match self.wallet.write().unwrap().open_message(&mut binding) {
            Ok(received) => {
                tracing::info!("get_sender_receiver: ReceivedTspMessage = {:?}", received);
                let sender = match &received {
                    ReceivedTspMessage::GenericMessage { sender, .. } => sender.clone(),
                    ReceivedTspMessage::RequestRelationship { sender, .. } => sender.clone(),
                    ReceivedTspMessage::AcceptRelationship { sender, .. } => sender.clone(),
                    ReceivedTspMessage::CancelRelationship { sender, .. } => sender.clone(),
                    ReceivedTspMessage::ForwardRequest { sender, .. } => sender.clone(),
                    ReceivedTspMessage::NewIdentifier { sender, .. } => sender.clone(),
                    ReceivedTspMessage::Referral { sender, .. } => sender.clone(),
                    ReceivedTspMessage::PendingMessage { .. } => {
                        return Err(anyhow::anyhow!("PendingMessage variant not supported"));
                    }
                };
                let receiver = match &received {
                    ReceivedTspMessage::GenericMessage { receiver, .. } => {
                        receiver.clone().unwrap_or_default()
                    }
                    ReceivedTspMessage::RequestRelationship { receiver, .. } => receiver.clone(),
                    ReceivedTspMessage::AcceptRelationship { receiver, .. } => receiver.clone(),
                    ReceivedTspMessage::CancelRelationship { receiver, .. } => receiver.clone(),
                    ReceivedTspMessage::ForwardRequest { receiver, .. } => receiver.clone(),
                    ReceivedTspMessage::NewIdentifier { receiver, .. } => receiver.clone(),
                    ReceivedTspMessage::Referral { receiver, .. } => receiver.clone(),
                    ReceivedTspMessage::PendingMessage { .. } => {
                        return Err(anyhow::anyhow!("PendingMessage variant not supported"));
                    }
                };
                Ok((sender, receiver))
            }
            Err(e) => {
                tracing::error!(
                    "get_sender_receiver: wallet.open_message error: {:?}, msg (hex): {:02x?}",
                    e,
                    msg
                );
                Err(e.into())
            }
        }
    }
    pub async fn new(alias: &str, settings: TmcpSettings) -> anyhow::Result<Self> {
        let wallet = Arc::new(RwLock::new(SecureStore::new()));
        let did = Self::init_identity(alias, &settings, &mut wallet.write().unwrap()).await?;
        tracing::info!("Create identity: alias = {}, did = {}", alias, did);

        Ok(Self {
            settings,
            wallet,
            did,
        })
    }

    async fn init_identity(
        alias: &str,
        settings: &TmcpSettings,
        wallet: &mut SecureStore,
    ) -> anyhow::Result<String> {
        if let Some(did) = wallet.resolve_alias(alias)? {
            // Verify DID still exists
            verify_vid(&did).await?;
            return Ok(did);
        }

        let name = format!("{}-{}", alias, Uuid::new_v4())
            .chars()
            .take(63)
            .collect::<String>();

        let domain = settings
            .did_webvh_format
            .split('/')
            .next()
            .unwrap_or_default();

        let path_name = name;
        let (_did_doc, private_doc, _) =
            tsp_sdk::vid::create_did_web(&path_name, domain, &settings.transport);
        let private_doc_str = serde_json::to_string(&private_doc)?;
        let identity: OwnedVid = serde_json::from_str(&private_doc_str)?;
        let did = identity.identifier().to_string();

        // Publish DID
        reqwest::Client::new()
            .post(&settings.did_publish_url)
            .json(&identity)
            .send()
            .await?
            .error_for_status()?;

        wallet.add_private_vid(identity, None)?;
        wallet.set_alias(alias.to_string(), did.clone())?;

        Ok(did)
    }

    pub async fn get_connection(&self, other_did: &str) -> anyhow::Result<TmcpConnection> {
        let verified_vid = verify_vid(other_did).await?.0;
        self.wallet
            .write()
            .unwrap()
            .add_verified_vid(verified_vid, None)?;
        tracing::info!(
            "Server get_connection: my_did = {}, other_did = {}",
            self.did,
            other_did
        );

        Ok(TmcpConnection::new(
            self.wallet.clone(),
            &self.did,
            other_did,
        ))
    }
}

#[derive(Clone)]
pub struct TmcpConnection {
    pub wallet: Arc<RwLock<SecureStore>>,
    my_did: String,
    other_did: String,
}

impl std::fmt::Debug for TmcpConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TmcpConnection")
            .field("my_did", &self.my_did)
            .field("other_did", &self.other_did)
            .finish()
    }
}

impl TmcpConnection {
    pub fn my_did(&self) -> &str {
        &self.my_did
    }
    pub fn new(wallet: Arc<RwLock<SecureStore>>, my_did: &str, other_did: &str) -> Self {
        Self {
            wallet,
            my_did: my_did.to_string(),
            other_did: other_did.to_string(),
        }
    }

    pub fn seal_message(&self, message: &str) -> anyhow::Result<String> {
        let wallet = self
            .wallet
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {:?}", e))?;
        let (_, tsp_message) = wallet
            .seal_message(&self.my_did, &self.other_did, None, message.as_bytes())
            .map_err(|e| anyhow::anyhow!("seal_message error: {:?}", e))?;

        Ok(URL_SAFE_NO_PAD.encode(tsp_message.as_ref() as &[u8]))
    }

    pub fn open_message(&self, encoded: &str) -> anyhow::Result<String> {
        tracing::debug!("TmcpConnection.open_message: encoded = {}", encoded);
        let mut tsp_message = match URL_SAFE_NO_PAD.decode(encoded) {
            Ok(msg) => msg,
            Err(e) => return Err(anyhow::anyhow!("base64 decode error: {:?}", e)),
        };

        let wallet = self
            .wallet
            .write()
            .map_err(|e| anyhow::anyhow!("RwLock poisoned: {:?}", e))?;
        let msg = wallet
            .open_message(&mut tsp_message)
            .map_err(|e| anyhow::anyhow!("wallet open_message error: {:?}", e))?;

        let sender = match &msg {
            ReceivedTspMessage::GenericMessage { sender, .. } => sender.as_str(),
            ReceivedTspMessage::RequestRelationship { sender, .. } => sender.as_str(),
            ReceivedTspMessage::AcceptRelationship { sender, .. } => sender.as_str(),
            ReceivedTspMessage::ForwardRequest { .. } => "",
            _ => "",
        };

        if !sender.is_empty() && sender != self.other_did {
            return Err(anyhow::anyhow!(
                "Received message from unexpected sender: {} (expected {})",
                sender,
                self.other_did
            ));
        }

        if let ReceivedTspMessage::GenericMessage { message, .. } = msg {
            let decoded = String::from_utf8_lossy(message).to_string();
            tracing::debug!("TmcpConnection.open_message: decoded = {}", decoded);
            Ok(decoded)
        } else {
            Err(anyhow::anyhow!("Expected GenericMessage, got {:?}", msg))
        }
    }
}

fn add_request_params(mut url: Url, did: &str) -> String {
    url.query_pairs_mut().append_pair("did", did);
    url.to_string()
}

pub async fn resolve_server(server_did: &str, did: Option<&str>) -> anyhow::Result<String> {
    let url_str = verify_vid(server_did).await?.0.endpoint().to_string();
    let mut url = Url::parse(&url_str)?;

    if let Some(did) = did {
        url = Url::parse(&add_request_params(url, did))?;
    }

    Ok(url.to_string())
}
