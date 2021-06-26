// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use super::error::Error;
use super::wire_msg::WireMsg;
#[cfg(not(feature = "no-igd"))]
use super::{api::DEFAULT_UPNP_LEASE_DURATION_SEC, igd::forward_port};
use super::{
    connection_deduplicator::ConnectionDeduplicator,
    connection_pool::ConnectionPool,
    connections::{
        listen_for_incoming_connections, listen_for_incoming_messages, Connection, RecvStream,
        SendStream,
    },
    error::Result,
    Config,
};
use bytes::Bytes;
use log::{debug, error, info, trace, warn};
use std::{net::SocketAddr, time::Duration};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::timeout;

/// Host name of the Quic communication certificate used by peers
// FIXME: make it configurable
const CERT_SERVER_NAME: &str = "MaidSAFE.net";

// Number of seconds before timing out the IGD request to forward a port.
#[cfg(not(feature = "no-igd"))]
const PORT_FORWARD_TIMEOUT: u64 = 30;

// Number of seconds before timing out the echo service query.
const ECHO_SERVICE_QUERY_TIMEOUT: u64 = 30;

/// Standard size of our channel bounds
const STANDARD_CHANNEL_SIZE: usize = 5;

/// Channel on which incoming messages can be listened to
pub struct IncomingMessages(pub(crate) Receiver<(SocketAddr, Bytes)>);

impl IncomingMessages {
    /// Blocks and returns the next incoming message and the source peer address
    pub async fn next(&mut self) -> Option<(SocketAddr, Bytes)> {
        self.0.recv().await
    }
}

/// Channel on which incoming connections are notified on
pub struct IncomingConnections(pub(crate) Receiver<SocketAddr>);

impl IncomingConnections {
    /// Blocks until there is an incoming connection and returns the address of the
    /// connecting peer
    pub async fn next(&mut self) -> Option<SocketAddr> {
        self.0.recv().await
    }
}

/// Disconnection
pub struct DisconnectionEvents(pub(crate) Receiver<SocketAddr>);

impl DisconnectionEvents {
    /// Blocks until there is a disconnection event and returns the address of the disconnected peer
    pub async fn next(&mut self) -> Option<SocketAddr> {
        self.0.recv().await
    }
}

/// Endpoint instance which can be used to create connections to peers,
/// and listen to incoming messages from other peers.
#[derive(Clone)]
pub struct Endpoint {
    local_addr: SocketAddr,
    public_addr: Option<SocketAddr>,
    quic_endpoint: quinn::Endpoint,
    message_tx: Sender<(SocketAddr, Bytes)>,
    disconnection_tx: Sender<SocketAddr>,
    client_cfg: quinn::ClientConfig,
    bootstrap_nodes: Vec<SocketAddr>,
    qp2p_config: Config,
    connection_pool: ConnectionPool,
    connection_deduplicator: ConnectionDeduplicator,
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("local_addr", &self.local_addr)
            .field("quic_endpoint", &"<endpoint omitted>")
            .field("client_cfg", &self.client_cfg)
            .finish()
    }
}

impl Endpoint {
    pub(crate) async fn new(
        quic_endpoint: quinn::Endpoint,
        quic_incoming: quinn::Incoming,
        client_cfg: quinn::ClientConfig,
        bootstrap_nodes: Vec<SocketAddr>,
        qp2p_config: Config,
    ) -> Result<(
        Self,
        IncomingConnections,
        IncomingMessages,
        DisconnectionEvents,
    )> {
        let local_addr = quic_endpoint.local_addr()?;
        let public_addr = match (qp2p_config.external_ip, qp2p_config.external_port) {
            (Some(ip), Some(port)) => Some(SocketAddr::new(ip, port)),
            _ => None,
        };
        let connection_pool = ConnectionPool::new();

        let (message_tx, message_rx) = mpsc::channel(STANDARD_CHANNEL_SIZE);
        let (connection_tx, connection_rx) = mpsc::channel(STANDARD_CHANNEL_SIZE);
        let (disconnection_tx, disconnection_rx) = mpsc::channel(STANDARD_CHANNEL_SIZE);

        let mut endpoint = Self {
            local_addr,
            public_addr,
            quic_endpoint,
            message_tx: message_tx.clone(),
            disconnection_tx: disconnection_tx.clone(),
            client_cfg,
            bootstrap_nodes,
            qp2p_config,
            connection_pool: connection_pool.clone(),
            connection_deduplicator: ConnectionDeduplicator::new(),
        };

        if let Some(addr) = endpoint.public_addr {
            // External IP and port number is provided
            // This means that the user has performed manual port-forwarding
            // Verify that the given socket address is reachable
            if let Some(contact) = endpoint.bootstrap_nodes.get(0) {
                info!("Verifying provided public IP address");
                endpoint.connect_to(contact).await?;
                let connection = endpoint
                    .get_connection(&contact)
                    .await
                    .ok_or(Error::MissingConnection)?;
                let (mut send, mut recv) = connection.open_bi().await?;
                send.send(WireMsg::EndpointVerificationReq(addr)).await?;
                let response = timeout(
                    Duration::from_secs(ECHO_SERVICE_QUERY_TIMEOUT),
                    WireMsg::read_from_stream(&mut recv.quinn_recv_stream),
                )
                .await;
                match response {
                    Ok(Ok(WireMsg::EndpointVerificationResp(valid))) => {
                        if valid {
                            info!("Endpoint verification successful! {} is reachable.", addr);
                        } else {
                            error!("Endpoint verification failed! {} is not reachable.", addr);
                            return Err(Error::IncorrectPublicAddress);
                        }
                    }
                    Ok(Ok(other)) => {
                        error!(
                            "Unexpected message when verifying public endpoint: {}",
                            other
                        );
                        return Err(Error::UnexpectedMessageType(other));
                    }
                    Ok(Err(err)) => {
                        error!("Error while verifying Public IP Address");
                        return Err(err);
                    }
                    Err(err) => {
                        error!(
                            "Timeout while trying to validate Public IP address: {}",
                            err
                        );
                        return Err(Error::IncorrectPublicAddress);
                    }
                }
            } else {
                warn!("Public IP address not verified since bootstrap contacts are empty");
            }
        } else {
            endpoint.public_addr = Some(endpoint.fetch_public_address().await?);
        }

        listen_for_incoming_connections(
            quic_incoming,
            connection_pool,
            message_tx,
            connection_tx,
            disconnection_tx,
            endpoint.clone(),
        );

        Ok((
            endpoint,
            IncomingConnections(connection_rx),
            IncomingMessages(message_rx),
            DisconnectionEvents(disconnection_rx),
        ))
    }

    /// Endpoint local address
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the socket address of the endpoint
    pub fn socket_addr(&self) -> SocketAddr {
        self.public_addr.unwrap_or(self.local_addr)
    }

    /// Get our connection address to give to others for them to connect to us.
    ///
    /// Attempts to use UPnP to automatically find the public endpoint and forward a port.
    /// Will use hard coded contacts to ask for our endpoint. If no contact is given then we'll
    /// simply build our connection info by querying the underlying bound socket for our address.
    /// Note that if such an obtained address is of unspecified category we will ignore that as
    /// such an address cannot be reached and hence not useful.
    async fn fetch_public_address(&mut self) -> Result<SocketAddr> {
        // Skip port forwarding
        if self.local_addr.ip().is_loopback() || !self.qp2p_config.forward_port {
            self.public_addr = Some(self.local_addr);
        }

        if let Some(socket_addr) = self.public_addr {
            return Ok(socket_addr);
        }

        let mut addr = None;

        #[cfg(feature = "no-igd")]
        if self.qp2p_config.forward_port {
            warn!("Ignoring 'forward_port' flag from config since IGD has been disabled (feature 'no-igd' has been set)");
        }

        // Try to contact an echo service
        match timeout(
            Duration::from_secs(ECHO_SERVICE_QUERY_TIMEOUT),
            self.query_ip_echo_service(),
        )
        .await
        {
            Ok(Ok(echo_res)) => addr = Some(echo_res),
            Ok(Err(err)) => info!("Could not contact echo service: {} - {:?}", err, err),
            Err(err) => info!("Query to echo service timed out: {:?}", err),
        }

        #[cfg(not(feature = "no-igd"))]
        if self.qp2p_config.forward_port {
            // Attempt to use IGD for port forwarding
            match timeout(
                Duration::from_secs(PORT_FORWARD_TIMEOUT),
                forward_port(
                    self.local_addr,
                    self.qp2p_config
                        .upnp_lease_duration
                        .unwrap_or(DEFAULT_UPNP_LEASE_DURATION_SEC),
                ),
            )
            .await
            {
                Ok(res) => match res {
                    Ok(port) => {
                        if let Some(socket) = &mut addr {
                            socket.set_port(port);
                        }
                    }
                    Err(e) => {
                        info!("IGD request failed: {} - {:?}", e, e);
                    }
                },
                Err(e) => {
                    info!("IGD request timeout: {:?}", e);
                }
            }
        }

        addr.map_or(Err(Error::UnresolvedPublicIp), |socket_addr| {
            self.public_addr = Some(socket_addr);
            Ok(socket_addr)
        })
    }

    /// Removes all existing connections to a given peer
    pub async fn disconnect_from(&self, peer_addr: &SocketAddr) -> Result<()> {
        self.connection_pool
            .remove(peer_addr)
            .await
            .iter()
            .for_each(|conn| {
                conn.close(0u8.into(), b"");
            });
        Ok(())
    }

    /// Connects to another peer.
    ///
    /// Returns `Connection` which is a handle for sending messages to the peer and
    /// `IncomingMessages` which is a stream of messages received from the peer.
    /// The incoming messages stream might be `None`. See the next section for more info.
    ///
    /// # Connection pooling
    ///
    /// Connection are stored in an internal pool and reused if possible. A connection remains in
    /// the pool while its `IncomingMessages` instances exists and while the connection is open.
    ///
    /// When a new connection is established, this function returns both the `Connection` instance
    /// and the `IncomingMessages` stream. If an existing connection is retrieved from the pool,
    /// the incoming messages will be `None`. Multiple `Connection` instances can exists
    /// simultaneously and they all share the same underlying connection resource. On the other
    /// hand, at most one `IncomingMessages` stream can exist per peer.
    ///
    /// How to handle the `IncomingMessages` depends on the networking model of the application:
    ///
    /// In the peer-to-peer model, where peers can arbitrarily send and receive messages to/from
    /// other peers, it is recommended to keep the `IncomingMessages` around and listen on it for
    /// new messages by repeatedly calling `next` and only drop it when it returns `None`.
    /// On the other hand, there is no need to keep `Connection` around as it can be cheaply
    /// retrieved again when needed by calling `connect_to`. When the connection gets closed by the
    /// peer or it timeouts due to inactivity, the incoming messages stream gets closed and once
    /// it's dropped the connection gets removed from the pool automatically. Calling `connect_to`
    /// afterwards will open a new connection.
    ///
    /// In the client-server model, where only the client send requests to the server and then
    /// listens for responses and never the other way around, it's OK to ignore (drop) the incoming
    /// messages stream and only use bi-directional streams obtained by calling
    /// `Connection::open_bi`. In this case the connection won't be pooled and the application is
    /// responsible for caching it.
    ///
    /// When sending a message on `Connection` fails, the connection is also automatically removed
    /// from the pool and the subsequent call to `connect_to` is guaranteed to reopen new connection
    /// too.
    pub async fn connect_to(&self, node_addr: &SocketAddr) -> Result<()> {
        if self.connection_pool.has(node_addr).await {
            trace!("We are already connected to this peer: {}", node_addr);
            return Ok(());
        }

        // Check if a connect attempt to this address is already in progress.
        match self.connection_deduplicator.query(node_addr).await {
            Some(Ok(())) => return Ok(()),
            Some(Err(error)) => return Err(error.into()),
            None => {}
        }

        // This is the first attempt - proceed with establishing the connection now.
        let connecting = match self.quic_endpoint.connect_with(
            self.client_cfg.clone(),
            node_addr,
            CERT_SERVER_NAME,
        ) {
            Ok(connecting) => connecting,
            Err(error) => {
                self.connection_deduplicator
                    .complete(node_addr, Err(error.clone().into()))
                    .await;
                return Err(error.into());
            }
        };

        let new_conn = match connecting.await {
            Ok(new_conn) => new_conn,
            Err(error) => {
                self.connection_deduplicator
                    .complete(node_addr, Err(error.clone().into()))
                    .await;
                return Err(error.into());
            }
        };

        trace!("Successfully connected to peer: {}", node_addr);

        self.add_new_connection_to_pool(new_conn).await;

        self.connection_deduplicator
            .complete(node_addr, Ok(()))
            .await;

        Ok(())
    }

    /// Verify if an address is publicly reachable. This will attempt to create
    /// a new connection and use it to exchange a message and verify that the node
    /// can be reached.
    pub async fn is_reachable(&self, peer_addr: &SocketAddr) -> Result<()> {
        let new_connection = self.create_new_connection(peer_addr).await?;
        let (mut send_stream, mut recv_stream) = new_connection.connection.open_bi().await?;
        let message = WireMsg::EndpointEchoReq;
        message.write_to_stream(&mut send_stream).await?;

        match timeout(
            Duration::from_secs(ECHO_SERVICE_QUERY_TIMEOUT),
            WireMsg::read_from_stream(&mut recv_stream),
        )
        .await
        {
            Ok(Ok(WireMsg::EndpointEchoResp(_))) => Ok(()),
            Ok(Ok(other)) => {
                info!(
                    "Unexpected message type when verifying reachability: {}",
                    &other
                );
                Ok(())
            }
            Ok(Err(err)) => {
                info!("Unable to contact peer: {:?}", err);
                Err(err)
            }
            Err(err) => {
                info!("Unable to contact peer: {:?}", err);
                Err(Error::NoEchoServiceResponse)
            }
        }
    }

    /// Creates a fresh connection without looking at the connection pool and connection duplicator.
    pub(crate) async fn create_new_connection(
        &self,
        peer_addr: &SocketAddr,
    ) -> Result<quinn::NewConnection> {
        let new_connection = self
            .quic_endpoint
            .connect_with(self.client_cfg.clone(), peer_addr, CERT_SERVER_NAME)?
            .await?;

        trace!("Successfully created new connection to peer: {}", peer_addr);
        Ok(new_connection)
    }

    pub(crate) async fn add_new_connection_to_pool(&self, conn: quinn::NewConnection) {
        let guard = self
            .connection_pool
            .insert(conn.connection.remote_address(), conn.connection)
            .await;

        listen_for_incoming_messages(
            conn.uni_streams,
            conn.bi_streams,
            guard,
            self.message_tx.clone(),
            self.disconnection_tx.clone(),
            self.clone(),
        );
    }

    /// Get an existing connection for the peer address.
    pub(crate) async fn get_connection(&self, peer_addr: &SocketAddr) -> Option<Connection> {
        if let Some((conn, guard)) = self.connection_pool.get(peer_addr).await {
            trace!("Connection exists in the connection pool: {}", peer_addr);
            Some(Connection::new(conn, guard))
        } else {
            None
        }
    }

    /// Open a bi-directional peer with a given peer
    pub async fn open_bidirectional_stream(
        &self,
        peer_addr: &SocketAddr,
    ) -> Result<(SendStream, RecvStream)> {
        self.connect_to(peer_addr).await?;
        let connection = self
            .get_connection(peer_addr)
            .await
            .ok_or(Error::MissingConnection)?;
        connection.open_bi().await
    }

    /// Sends a message to a peer. This will attempt to use an existing connection
    /// to the destination  peer. If a connection does not exist, this will fail with `Error::MissingConnection`
    pub async fn try_send_message(&self, msg: Bytes, dest: &SocketAddr) -> Result<()> {
        let connection = self
            .get_connection(dest)
            .await
            .ok_or(Error::MissingConnection)?;
        connection.send_uni(msg).await?;
        Ok(())
    }

    /// Sends a message to a peer. This will attempt to use an existing connection
    /// to the peer first. If this connection is broken or doesn't exist
    /// a new connection is created and the message is sent.
    pub async fn send_message(&self, msg: Bytes, dest: &SocketAddr) -> Result<()> {
        if self.try_send_message(msg.clone(), dest).await.is_ok() {
            return Ok(());
        }
        self.connect_to(dest).await?;
        self.try_send_message(msg, dest).await
    }

    /// Close all the connections of this endpoint immediately and stop accepting new connections.
    pub fn close(&self) {
        self.quic_endpoint.close(0_u32.into(), b"")
    }

    // Private helper
    async fn query_ip_echo_service(&self) -> Result<SocketAddr> {
        // Bail out early if we don't have any contacts.
        if self.bootstrap_nodes.is_empty() {
            return Err(Error::NoEchoServerEndpointDefined);
        }

        let mut tasks = Vec::default();
        for node in self.bootstrap_nodes.iter().cloned() {
            let endpoint = self.clone();
            let task_handle = tokio::spawn(async move {
                debug!("Connecting to {:?}", &node);
                endpoint.connect_to(&node).await?;
                let connection = endpoint
                    .get_connection(&node)
                    .await
                    .ok_or(Error::MissingConnection)?;
                let (mut send_stream, mut recv_stream) = connection.open_bi().await?;
                send_stream.send(WireMsg::EndpointEchoReq).await?;
                match WireMsg::read_from_stream(&mut recv_stream.quinn_recv_stream).await {
                    Ok(WireMsg::EndpointEchoResp(socket_addr)) => Ok(socket_addr),
                    Ok(msg) => Err(Error::UnexpectedMessageType(msg)),
                    Err(err) => Err(err),
                }
            });
            tasks.push(task_handle);
        }

        let (result, _) = futures::future::select_ok(tasks).await.map_err(|err| {
            error!("Failed to contact echo service: {}", err);
            Error::EchoServiceFailure(err.to_string())
        })?;

        result
    }

    /// Returns the list of boostrap nodes that the endpoint will try bootstrapping to.
    /// This is the combined list of contacts from the Hard Coded contacts in the config
    /// and the bootstrap cache (if enabled)
    pub fn bootstrap_nodes(&self) -> &[SocketAddr] {
        &self.bootstrap_nodes
    }
}
