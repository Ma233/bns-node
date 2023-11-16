#![warn(missing_docs)]

//! Processor of rings-node jsonrpc-server.

use std::str::FromStr;
use std::sync::Arc;

use futures::future::Join;
use futures::Future;
use rings_core::message::MessagePayload;
use rings_core::swarm::impls::ConnectionHandshake;
use rings_transport::core::transport::ConnectionInterface;
use serde::Deserialize;
use serde::Serialize;

use crate::backend::types::BackendMessage;
use crate::backend::types::MessageType;
use crate::consts::DATA_REDUNDANT;
use crate::error::Error;
use crate::error::Result;
use crate::measure::PeriodicMeasure;
use crate::prelude::http;
use crate::prelude::jsonrpc_client::SimpleClient;
use crate::prelude::jsonrpc_core;
use crate::prelude::rings_core::dht::Did;
use crate::prelude::rings_core::dht::Stabilization;
use crate::prelude::rings_core::dht::TStabilize;
use crate::prelude::rings_core::message::Decoder;
use crate::prelude::rings_core::message::Encoded;
use crate::prelude::rings_core::message::Encoder;
use crate::prelude::rings_core::message::Message;
use crate::prelude::rings_core::message::PayloadSender;
use crate::prelude::rings_core::prelude::uuid;
use crate::prelude::rings_core::storage::PersistenceStorage;
use crate::prelude::rings_core::swarm::MeasureImpl;
use crate::prelude::rings_core::swarm::Swarm;
use crate::prelude::rings_core::swarm::SwarmBuilder;
use crate::prelude::rings_rpc::method;
use crate::prelude::rings_rpc::response;
use crate::prelude::rings_rpc::types::HttpRequest;
use crate::prelude::rings_rpc::types::Timeout;
use crate::prelude::vnode;
use crate::prelude::wasm_export;
use crate::prelude::ChordStorageInterface;
use crate::prelude::ChordStorageInterfaceCacheChecker;
use crate::prelude::Connection;
use crate::prelude::CustomMessage;
use crate::prelude::SessionSk;

/// ProcessorConfig is usually serialized as json or yaml.
/// There is a `from_config` method in [ProcessorBuilder] used to initialize the Builder with a serialized ProcessorConfig.
#[derive(Clone)]
#[wasm_export]
pub struct ProcessorConfig {
    /// ICE servers for webrtc
    ice_servers: String,
    /// External address for webrtc
    external_address: Option<String>,
    /// [SessionSk].
    session_sk: SessionSk,
    /// Stabilization timeout.
    stabilize_timeout: usize,
}

#[wasm_export]
impl ProcessorConfig {
    /// Creates a new `ProcessorConfig` instance without an external address.
    pub fn new(ice_servers: String, session_sk: SessionSk, stabilize_timeout: usize) -> Self {
        Self {
            ice_servers,
            external_address: None,
            session_sk,
            stabilize_timeout,
        }
    }

    /// Creates a new `ProcessorConfig` instance with an external address.
    pub fn new_with_ext_addr(
        ice_servers: String,
        session_sk: SessionSk,
        stabilize_timeout: usize,
        external_address: String,
    ) -> Self {
        Self {
            ice_servers,
            external_address: Some(external_address),
            session_sk,
            stabilize_timeout,
        }
    }

    /// Return associated [SessionSk].
    pub fn session_sk(&self) -> SessionSk {
        self.session_sk.clone()
    }
}

impl FromStr for ProcessorConfig {
    type Err = Error;
    /// Reveal config from serialized string.
    fn from_str(ser: &str) -> Result<Self> {
        serde_yaml::from_str::<ProcessorConfig>(ser).map_err(Error::SerdeYamlError)
    }
}

/// `ProcessorConfigSerialized` is a serialized version of `ProcessorConfig`.
/// Instead of storing the `SessionSk` instance, it stores the dumped string representation of the session secret key.
#[derive(Serialize, Deserialize, Clone)]
#[wasm_export]
pub struct ProcessorConfigSerialized {
    /// A string representing ICE servers for WebRTC
    ice_servers: String,
    /// An optional string representing the external address for WebRTC
    external_address: Option<String>,
    /// A string representing the dumped `SessionSk`.
    session_sk: String,
    /// An unsigned integer representing the stabilization timeout.
    stabilize_timeout: usize,
}

impl ProcessorConfigSerialized {
    /// Creates a new `ProcessorConfigSerialized` instance without an external address.
    pub fn new(ice_servers: String, session_sk: String, stabilize_timeout: usize) -> Self {
        Self {
            ice_servers,
            external_address: None,
            session_sk,
            stabilize_timeout,
        }
    }

    /// Creates a new `ProcessorConfigSerialized` instance with an external address.
    pub fn new_with_ext_addr(
        ice_servers: String,
        session_sk: String,
        stabilize_timeout: usize,
        external_address: String,
    ) -> Self {
        Self {
            ice_servers,
            external_address: Some(external_address),
            session_sk,
            stabilize_timeout,
        }
    }
}

impl TryFrom<ProcessorConfig> for ProcessorConfigSerialized {
    type Error = Error;
    fn try_from(ins: ProcessorConfig) -> Result<Self> {
        Ok(Self {
            ice_servers: ins.ice_servers.clone(),
            external_address: ins.external_address.clone(),
            session_sk: ins.session_sk.dump()?,
            stabilize_timeout: ins.stabilize_timeout,
        })
    }
}

impl TryFrom<ProcessorConfigSerialized> for ProcessorConfig {
    type Error = Error;
    fn try_from(ins: ProcessorConfigSerialized) -> Result<Self> {
        Ok(Self {
            ice_servers: ins.ice_servers.clone(),
            external_address: ins.external_address.clone(),
            session_sk: SessionSk::from_str(&ins.session_sk)?,
            stabilize_timeout: ins.stabilize_timeout,
        })
    }
}

impl Serialize for ProcessorConfig {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> core::result::Result<S::Ok, S::Error> {
        let ins: ProcessorConfigSerialized = self
            .clone()
            .try_into()
            .map_err(|e: Error| serde::ser::Error::custom(e.to_string()))?;
        ProcessorConfigSerialized::serialize(&ins, serializer)
    }
}

impl<'de> serde::de::Deserialize<'de> for ProcessorConfig {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        match ProcessorConfigSerialized::deserialize(deserializer) {
            Ok(ins) => {
                let cfg: ProcessorConfig = ins
                    .try_into()
                    .map_err(|e: Error| serde::de::Error::custom(e.to_string()))?;
                Ok(cfg)
            }
            Err(e) => Err(e),
        }
    }
}

/// ProcessorBuilder is used to initialize a [Processor] instance.
pub struct ProcessorBuilder {
    ice_servers: String,
    external_address: Option<String>,
    session_sk: SessionSk,
    storage: Option<PersistenceStorage>,
    measure: Option<MeasureImpl>,
    stabilize_timeout: usize,
}

/// Processor for rings-node jsonrpc server
#[derive(Clone)]
pub struct Processor {
    /// a swarm instance
    pub swarm: Arc<Swarm>,
    /// a stabilization instance,
    pub stabilization: Arc<Stabilization>,
}

impl ProcessorBuilder {
    /// initialize a [ProcessorBuilder] with a serialized [ProcessorConfig].
    pub fn from_serialized(config: &str) -> Result<Self> {
        let config =
            serde_yaml::from_str::<ProcessorConfig>(config).map_err(Error::SerdeYamlError)?;
        Self::from_config(&config)
    }

    /// initialize a [ProcessorBuilder] with a [ProcessorConfig].
    pub fn from_config(config: &ProcessorConfig) -> Result<Self> {
        Ok(Self {
            ice_servers: config.ice_servers.clone(),
            external_address: config.external_address.clone(),
            session_sk: config.session_sk.clone(),
            storage: None,
            measure: None,
            stabilize_timeout: config.stabilize_timeout,
        })
    }

    /// Set the storage for the processor.
    pub fn storage(mut self, storage: PersistenceStorage) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set the measure for the processor.
    pub fn measure(mut self, implement: PeriodicMeasure) -> Self {
        self.measure = Some(Box::new(implement));
        self
    }

    /// Build the [Processor].
    pub fn build(self) -> Result<Processor> {
        self.session_sk
            .session()
            .verify_self()
            .map_err(|e| Error::VerifyError(e.to_string()))?;

        let storage = self
            .storage
            .expect("Please set storage by `storage()` method");

        let mut swarm_builder = SwarmBuilder::new(&self.ice_servers, storage, self.session_sk);

        if let Some(external_address) = self.external_address {
            swarm_builder = swarm_builder.external_address(external_address);
        }

        if let Some(measure) = self.measure {
            swarm_builder = swarm_builder.measure(measure);
        }

        let swarm = Arc::new(swarm_builder.build());
        let stabilization = Arc::new(Stabilization::new(swarm.clone(), self.stabilize_timeout));

        Ok(Processor {
            swarm,
            stabilization,
        })
    }
}

impl Processor {
    /// Listen processor message
    pub fn listen(&self) -> Join<impl Future, impl Future> {
        let swarm = self.swarm.clone();
        let message_listener = async { swarm.listen().await };

        let stb = self.stabilization.clone();
        let stabilization = async { stb.wait().await };

        futures::future::join(message_listener, stabilization)
    }
}

impl Processor {
    /// Get current did
    pub fn did(&self) -> Did {
        self.swarm.did()
    }

    /// Connect peer with remote rings-node jsonrpc server.
    /// * peer_url: the remote rings-node jsonrpc server url.
    pub async fn connect_peer_via_http(&self, peer_url: &str) -> Result<Peer> {
        // request remote offer and sand answer to remote
        tracing::debug!("connect_peer_via_http: {}", peer_url);

        let client = SimpleClient::new(peer_url, None);

        let did_resp = client
            .call_method(method::Method::NodeDid.as_str(), jsonrpc_core::Params::None)
            .await
            .map_err(|e| Error::RemoteRpcError(e.to_string()))?;
        let did = serde_json::from_value::<String>(did_resp)
            .map_err(|_| Error::InvalidDid)?
            .parse()
            .map_err(|_| Error::InvalidDid)?;

        let (_, offer) = self
            .swarm
            .create_offer(did)
            .await
            .map_err(Error::CreateOffer)?;
        let encoded_offer = offer.encode().map_err(|_| Error::EncodeError)?;
        tracing::debug!("sending encoded offer {:?} to {}", encoded_offer, peer_url);
        let req: serde_json::Value = serde_json::to_value(encoded_offer)
            .map_err(Error::SerdeJsonError)
            .map_err(Error::from)?;

        let resp = client
            .call_method(
                method::Method::AnswerOffer.as_str(),
                jsonrpc_core::Params::Array(vec![req]),
            )
            .await
            .map_err(|e| Error::RemoteRpcError(e.to_string()))?;

        let answer_payload_str: String =
            serde_json::from_value(resp).map_err(|_| Error::EncodeError)?;

        let encoded_answer: Encoded = <Encoded as From<&str>>::from(&answer_payload_str);

        let answer_payload =
            MessagePayload::from_encoded(&encoded_answer).map_err(|_| Error::DecodeError)?;

        let (did, conn) = self
            .swarm
            .accept_answer(answer_payload)
            .await
            .map_err(Error::AcceptAnswer)?;

        Ok(Peer::from((did, conn)))
    }

    /// Connect peer with web3 did.
    /// There are 3 peers: PeerA, PeerB, PeerC.
    /// 1. PeerA has a connection with PeerB.
    /// 2. PeerC has a connection with PeerB.
    /// 3. PeerC can connect PeerA with PeerA's web3 address.
    pub async fn connect_with_did(&self, did: Did, wait_for_open: bool) -> Result<Peer> {
        let conn = self.swarm.connect(did).await.map_err(Error::ConnectError)?;
        tracing::debug!("wait for connection connected");
        if wait_for_open {
            conn.webrtc_wait_for_data_channel_open()
                .await
                .map_err(|e| Error::ConnectError(rings_core::error::Error::Transport(e)))?;
        }
        Ok(Peer::from((did, conn)))
    }

    /// List all peers.
    pub async fn list_peers(&self) -> Result<Vec<Peer>> {
        let conns = self.swarm.get_connections();
        tracing::debug!(
            "addresses: {:?}",
            conns.iter().map(|(a, _b)| a).collect::<Vec<_>>()
        );
        let data = conns.iter().map(|x| x.into()).collect::<Vec<Peer>>();
        Ok(data)
    }

    /// Get peer by remote did
    pub async fn get_peer(&self, did: Did) -> Result<Peer> {
        let conn = self
            .swarm
            .get_connection(did)
            .ok_or(Error::ConnectionNotFound)?;
        Ok(Peer::from(&(did, conn)))
    }

    /// Disconnect a peer with web3 did.
    pub async fn disconnect(&self, did: Did) -> Result<()> {
        self.swarm
            .disconnect(did)
            .await
            .map_err(Error::CloseConnectionError)
    }

    /// Disconnect all connections.
    pub async fn disconnect_all(&self) {
        let dids = self.swarm.get_connection_ids();

        let close_async = dids
            .into_iter()
            .map(|did| self.swarm.disconnect(did))
            .collect::<Vec<_>>();

        futures::future::join_all(close_async).await;
    }

    /// Send custom message to a did.
    pub async fn send_message(&self, destination: &str, msg: &[u8]) -> Result<uuid::Uuid> {
        tracing::info!(
            "send_message, destination: {}, text: {:?}",
            destination,
            msg,
        );
        let destination = Did::from_str(destination).map_err(|_| Error::InvalidDid)?;

        let mut new_msg = Vec::with_capacity(msg.len() + 4);
        // chunked mark
        new_msg.push(0);
        new_msg.extend_from_slice(&[0u8; 3]);
        new_msg.extend_from_slice(msg);

        let msg = Message::custom(&new_msg).map_err(Error::SendMessage)?;

        let uuid = self
            .swarm
            .send_message(msg, destination)
            .await
            .map_err(Error::SendMessage)?;
        Ok(uuid)
    }

    /// send http request message to node
    /// - destination: did of destination
    /// - url: ipfs url
    /// - timeout: timeout in millisecond
    #[allow(clippy::too_many_arguments)]
    pub async fn send_http_request_message<U, T>(
        &self,
        destination: &str,
        name: U,
        method: http::Method,
        url: U,
        timeout: T,
        headers: &[(U, U)],
        body: Option<Vec<u8>>,
    ) -> Result<uuid::Uuid>
    where
        U: ToString,
        T: Into<Timeout>,
    {
        let timeout: Timeout = timeout.into();
        tracing::info!(
            "send_http_request_message, destination: {}, url: {:?}, timeout: {:?}",
            destination,
            url.to_string(),
            timeout,
        );
        let msg: BackendMessage = BackendMessage::try_from((
            MessageType::HttpRequest,
            &HttpRequest::new(name, method, url, timeout, headers, body),
        ))?;
        let msg: Vec<u8> = msg.into();

        self.send_message(destination, &msg).await
    }

    /// send simple text message
    /// - destination: did of destination
    /// - text: text message
    pub async fn send_simple_text_message(
        &self,
        destination: &str,
        text: &str,
    ) -> Result<uuid::Uuid> {
        tracing::info!(
            "send_simple_text_message, destination: {}, text: {:?}",
            destination,
            text,
        );

        let msg: BackendMessage =
            BackendMessage::from((MessageType::SimpleText.into(), text.as_bytes()));
        let msg: Vec<u8> = msg.into();
        self.send_message(destination, &msg).await
    }

    /// send custom message
    /// - destination: did of destination
    /// - message_type: custom message type u16
    /// - extra: extra data
    /// - data: payload data
    pub async fn send_custom_message(
        &self,
        destination: &str,
        message_type: u16,
        data: Vec<u8>,
        extra: [u8; 30],
    ) -> Result<uuid::Uuid> {
        tracing::info!(
            "send_custom_message, destination: {}, message_type: {}",
            destination,
            message_type,
        );

        let msg: BackendMessage = BackendMessage::new(message_type, extra, data.as_ref());
        let msg: Vec<u8> = msg.into();
        self.send_message(destination, &msg[..]).await
    }

    /// check local cache of dht
    pub async fn storage_check_cache(&self, did: Did) -> Option<vnode::VirtualNode> {
        self.swarm.storage_check_cache(did).await
    }

    /// fetch virtual node from DHT
    pub async fn storage_fetch(&self, did: Did) -> Result<()> {
        <Swarm as ChordStorageInterface<DATA_REDUNDANT>>::storage_fetch(&self.swarm, did)
            .await
            .map_err(Error::VNodeError)
    }

    /// store virtual node on DHT
    pub async fn storage_store(&self, vnode: vnode::VirtualNode) -> Result<()> {
        <Swarm as ChordStorageInterface<DATA_REDUNDANT>>::storage_store(&self.swarm, vnode)
            .await
            .map_err(Error::VNodeError)
    }

    /// append data to a virtual node on DHT
    pub async fn storage_append_data(&self, topic: &str, data: Encoded) -> Result<()> {
        <Swarm as ChordStorageInterface<DATA_REDUNDANT>>::storage_append_data(
            &self.swarm,
            topic,
            data,
        )
        .await
        .map_err(Error::VNodeError)
    }

    /// register service
    pub async fn register_service(&self, name: &str) -> Result<()> {
        let encoded_did = self
            .did()
            .to_string()
            .encode()
            .map_err(Error::ServiceRegisterError)?;
        <Swarm as ChordStorageInterface<DATA_REDUNDANT>>::storage_touch_data(
            &self.swarm,
            name,
            encoded_did,
        )
        .await
        .map_err(Error::ServiceRegisterError)
    }

    /// get node info
    pub async fn get_node_info(&self) -> Result<response::NodeInfo> {
        Ok(response::NodeInfo {
            version: crate::util::build_version(),
            swarm: self.swarm.inspect().await,
        })
    }
}

/// Peer struct
#[derive(Clone)]
pub struct Peer {
    /// web3 did of a peer.
    pub did: String,
    /// the connection.
    pub connection: Connection,
}

impl From<(Did, Connection)> for Peer {
    fn from((did, connection): (Did, Connection)) -> Self {
        Self {
            did: did.to_string(),
            connection,
        }
    }
}

impl From<&(Did, Connection)> for Peer {
    fn from((did, connection): &(Did, Connection)) -> Self {
        Self {
            did: did.to_string(),
            connection: connection.clone(),
        }
    }
}

impl Peer {
    /// convert peer to response peer
    pub fn into_response_peer(&self) -> rings_rpc::response::Peer {
        rings_rpc::response::Peer {
            did: self.did.clone(),
            cid: self.did.clone(),
            state: format!("{:?}", self.connection.webrtc_connection_state()),
        }
    }
}

/// unpack custom message to text
pub fn unpack_text_message(msg: &CustomMessage) -> Result<String> {
    let (left, right) = msg.0.split_at(4);
    if left[0] != 0 {
        return Err(Error::InvalidData);
    }
    let text = String::from_utf8(right.to_vec()).unwrap();
    Ok(text)
}

#[cfg(test)]
#[cfg(feature = "node")]
mod test {
    use futures::lock::Mutex;
    use rings_core::swarm::callback::SwarmCallback;
    use rings_transport::core::transport::WebrtcConnectionState;

    use super::*;
    use crate::prelude::*;
    use crate::tests::native::prepare_processor;

    #[tokio::test]
    async fn test_processor_create_offer() {
        let peer_did = SecretKey::random().address().into();
        let (processor, path) = prepare_processor().await;
        processor.swarm.create_offer(peer_did).await.unwrap();
        let conn_dids = processor.swarm.get_connection_ids();
        assert_eq!(conn_dids.len(), 1);
        assert_eq!(conn_dids.get(0).unwrap(), &peer_did);
        tokio::fs::remove_dir_all(path).await.unwrap();
    }

    struct SwarmCallbackInstance {
        pub msgs: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl SwarmCallback for SwarmCallbackInstance {
        async fn on_inbound(
            &self,
            payload: &MessagePayload,
        ) -> std::result::Result<(), Box<dyn std::error::Error>> {
            let msg: Message = payload.transaction.data().map_err(Box::new)?;

            if let Message::CustomMessage(ref msg) = msg {
                let text = unpack_text_message(msg).unwrap();
                let mut msgs = self.msgs.try_lock().unwrap();
                msgs.push(text);
            }

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_processor_handshake_msg() {
        let callback1 = Arc::new(SwarmCallbackInstance {
            msgs: Mutex::new(Vec::new()),
        });
        let callback2 = Arc::new(SwarmCallbackInstance {
            msgs: Mutex::new(Vec::new()),
        });

        let (p1, path1) = prepare_processor().await;
        let (p2, path2) = prepare_processor().await;

        p1.swarm.set_callback(callback1.clone()).unwrap();
        p2.swarm.set_callback(callback2.clone()).unwrap();

        let did1 = p1.did().to_string();
        let did2 = p2.did().to_string();

        println!("p1_did: {}", did1);
        println!("p2_did: {}", did2);

        let swarm1 = p1.swarm.clone();
        let swarm2 = p2.swarm.clone();
        tokio::spawn(async { swarm1.listen().await });
        tokio::spawn(async { swarm2.listen().await });

        let (conn1, offer) = p1.swarm.create_offer(p2.did()).await.unwrap();
        assert_eq!(
            p1.swarm
                .get_connection(p2.did())
                .unwrap()
                .webrtc_connection_state(),
            WebrtcConnectionState::New,
        );

        let (conn2, answer) = p2.swarm.answer_offer(offer).await.unwrap();
        let (peer_did, _) = p1.swarm.accept_answer(answer).await.unwrap();
        assert!(
            peer_did.to_string().eq(&did2),
            "peer.address got {}, expect: {}",
            peer_did,
            did2
        );

        println!("waiting for connection");
        conn1.webrtc_wait_for_data_channel_open().await.unwrap();

        assert!(conn1.is_connected().await, "conn1 not connected");
        assert!(
            p1.swarm
                .get_connection(p2.did())
                .unwrap()
                .is_connected()
                .await,
            "p1 connection not connected"
        );
        assert!(conn2.is_connected().await, "conn2 not connected");
        assert!(
            p2.swarm
                .get_connection(p1.did())
                .unwrap()
                .is_connected()
                .await,
            "p2 connection not connected"
        );

        println!("waiting for data channel ready");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        let test_text1 = "test1";
        let test_text2 = "test2";

        println!("send_message 1");
        let uuid1 = p1
            .send_message(did2.as_str(), test_text1.as_bytes())
            .await
            .unwrap();
        println!("send_message 1 done, msg id: {}", uuid1);

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        println!("send_message 2");
        let uuid2 = p2
            .send_message(did1.as_str(), test_text2.as_bytes())
            .await
            .unwrap();
        println!("send_message 2 done, msg id: {}", uuid2);

        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        println!("check received");

        let mut msgs2 = callback2.msgs.try_lock().unwrap();
        let got_msg2 = msgs2.pop().unwrap();
        assert!(
            got_msg2.eq(test_text1),
            "msg received, expect {}, got {}",
            test_text1,
            got_msg2
        );

        let mut msgs1 = callback1.msgs.try_lock().unwrap();
        let got_msg1 = msgs1.pop().unwrap();
        assert!(
            got_msg1.eq(test_text2),
            "msg received, expect {}, got {}",
            test_text2,
            got_msg1
        );
        tokio::fs::remove_dir_all(path1).await.unwrap();
        tokio::fs::remove_dir_all(path2).await.unwrap();
    }
}
