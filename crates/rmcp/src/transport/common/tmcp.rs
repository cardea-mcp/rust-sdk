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
    pub verbose: bool,
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
            verbose: true,
            wallet_url: "sqlite://wallet.sqlite".into(),
            wallet_password: "unsecure".into(),
            use_webvh: true,
        }
    }
}

pub struct TmcpIdentityManager {
    settings: TmcpSettings,
    wallet: SecureStore,
    pub did: String,
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
    pub async fn new(alias: &str, settings: TmcpSettings) -> anyhow::Result<Self> {
        let mut wallet = SecureStore::new();
        let did = Self::init_identity(alias, &settings, &mut wallet).await?;

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

        wallet.add_private_vid(identity)?;
        wallet.set_alias(alias.to_string(), did.clone())?;

        Ok(did)
    }

    pub async fn get_connection(&self, other_did: &str) -> anyhow::Result<TmcpConnection> {
        let verified_vid = verify_vid(other_did).await?;
        self.wallet.add_verified_vid(verified_vid)?;

        Ok(TmcpConnection::new(
            self.wallet.clone(),
            &self.did,
            other_did,
            self.settings.verbose,
        ))
    }
}

pub struct TmcpConnection {
    wallet: SecureStore,
    my_did: String,
    other_did: String,
    verbose: bool,
}

impl Clone for TmcpConnection {
    fn clone(&self) -> Self {
        Self {
            wallet: self.wallet.clone(),
            my_did: self.my_did.clone(),
            other_did: self.other_did.clone(),
            verbose: self.verbose,
        }
    }
}

impl TmcpConnection {
    pub fn new(wallet: SecureStore, my_did: &str, other_did: &str, verbose: bool) -> Self {
        Self {
            wallet,
            my_did: my_did.to_string(),
            other_did: other_did.to_string(),
            verbose,
        }
    }

    pub fn seal_message(&self, message: &str) -> anyhow::Result<String> {
        if self.verbose {
            // info!("Encoding TSP message: {}", message);
        }

        let (_, tsp_message) =
            self.wallet
                .seal_message(&self.my_did, &self.other_did, None, message.as_bytes())?;

        if self.verbose {
            // println!("{}", tsp::color_print(&tsp_message));
        }

        Ok(URL_SAFE_NO_PAD.encode(tsp_message.as_ref() as &[u8]))
    }

    pub fn open_message(&self, encoded: &str) -> anyhow::Result<String> {
        let mut tsp_message = URL_SAFE_NO_PAD.decode(encoded)?;

        if self.verbose {
            // println!("{}", tsp::color_print(&tsp_message));
        }

        let msg = self.wallet.open_message(&mut tsp_message)?;

        let sender = match &msg {
            ReceivedTspMessage::GenericMessage { sender, .. } => sender.as_str(),
            ReceivedTspMessage::RequestRelationship { sender, .. } => sender.as_str(),
            ReceivedTspMessage::AcceptRelationship { sender, .. } => sender.as_str(),
            ReceivedTspMessage::ForwardRequest { .. } => "",
            _ => "",
        };

        if !sender.is_empty() && sender != self.other_did {
            // warn!("Received message from unexpected sender: {} (expected {})", sender, self.other_did);
        }

        if let ReceivedTspMessage::GenericMessage { message, .. } = msg {
            Ok(String::from_utf8_lossy(message).to_string())
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
    let url_str = verify_vid(server_did).await?.endpoint().to_string();
    let mut url = Url::parse(&url_str)?;

    if let Some(did) = did {
        url = Url::parse(&add_request_params(url, did))?;
    }

    Ok(url.to_string())
}
