// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

//! This example connects to bootstrap node, waits for other client node contacts and starts
//! communicating with them.
//!
//! Usage:
//! ```
//! $ RUST_LOG=client_node=info cargo run --example client_node -- --hard-coded-contacts
//! '[{"peer_addr": "127.0.0.1:5000","peer_cert_der":[48,130,..]}]'
//! ```

mod common;

use anyhow::{Context, Result};
use bytes::Bytes;
use common::{Event, EventReceivers, Rpc};
use crc::crc32;
use log::{debug, error, info};
use qp2p::{Config, Endpoint, QuicP2p};
use rand::{self, distributions::Standard, seq::IteratorRandom, Rng};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use structopt::StructOpt;
use tracing_subscriber::EnvFilter;

/// Client node will be connecting to bootstrap node from which it will receive contacts
/// of other client nodes. Then this node will connect with all other client nodes and
/// try to exchange some data with them.
#[derive(Debug, StructOpt)]
struct CliArgs {
    #[structopt(flatten)]
    quic_p2p_opts: Config,
}

struct ClientNode {
    endpoint: Endpoint,
    bootstrap_node_addr: SocketAddr,
    event_rx: EventReceivers,
    /// Other nodes we will be communicating with.
    client_nodes: HashSet<SocketAddr>,
    our_addr: SocketAddr,
    sent_messages: usize,
    received_messages: usize,
    /// Large message to send
    large_msg: Bytes,
    /// Smaller message to send
    small_msg: Bytes,
    peer_states: HashMap<SocketAddr, bool>,
}

const LARGE_MSG_SIZE: usize = 20 * 1024 * 1024; // 20 MB
const SMALL_MSG_SIZE: usize = 16 * 1024; // 16 KB

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = CliArgs::from_args();
    println!("{:?}", config);

    match ClientNode::new(config.quic_p2p_opts).await {
        Ok(mut c) => c.run().await?,
        Err(e) => eprintln!("{}", e),
    }
    Ok(())
}

impl ClientNode {
    async fn new(config: Config) -> Result<Self> {
        // Choose a random bootstrap node.
        let bootstrap_node_addr = *config
            .hard_coded_contacts
            .iter()
            .choose(&mut rand::thread_rng())
            .with_context(|| "No valid bootstrap node was provided.".to_string())?;

        // let (event_tx, event_rx) = new_unbounded_channels();
        let qp2p = QuicP2p::with_config(Some(config), &[], false)?;

        let large_msg = Bytes::from(random_data_with_hash(LARGE_MSG_SIZE));
        assert!(hash_correct(&large_msg));

        let small_msg = Bytes::from(random_data_with_hash(SMALL_MSG_SIZE));
        assert!(hash_correct(&small_msg));

        let (endpoint, incoming_connections, incoming_messages, disconnections) =
            qp2p.new_endpoint().await?;
        let our_addr = endpoint.socket_addr();
        info!("Our address: {}", our_addr);

        let event_rx = EventReceivers {
            incoming_connections,
            incoming_messages,
            disconnections,
        };

        Ok(Self {
            endpoint,
            bootstrap_node_addr,
            large_msg,
            small_msg,
            client_nodes: Default::default(),
            our_addr,
            peer_states: Default::default(),
            sent_messages: 0,
            received_messages: 0,
            event_rx,
        })
    }

    /// Blocks and reacts to qp2p events.
    async fn run(&mut self) -> Result<()> {
        info!("Peer started");

        // this dummy send will trigger connection
        let bootstrap_node = self.bootstrap_node_addr;
        // TODO: handle tokens properly. Currently just hardcoding to 0 in example
        self.endpoint.connect_to(&bootstrap_node).await?;

        self.poll_qp2p_events().await
    }

    async fn poll_qp2p_events(&mut self) -> Result<()> {
        while let Some(event) = self.event_rx.recv().await {
            match event {
                Event::ConnectedTo { addr } => self.on_connect(addr).await?,
                Event::NewMessage { src, msg } => self.on_msg_receive(src, msg).await?,
                // event => warn!("Unexpected event: {:?}", event),
            }
        }
        Ok(())
    }

    async fn on_connect(&mut self, peer: SocketAddr) -> Result<()> {
        info!("Connected with: {}", peer);

        if peer == self.bootstrap_node_addr {
            info!("Connected to bootstrap node. Waiting for other node contacts...");
        } else if self.client_nodes.contains(&peer) {
            // TODO: handle tokens properly. Currently just hardcoding to 0 in example
            self.endpoint
                .send_message(self.large_msg.clone(), &peer)
                .await?;
            self.endpoint
                .send_message(self.small_msg.clone(), &peer)
                .await?;
            self.sent_messages += 1;
        }
        Ok(())
    }

    async fn on_msg_receive(&mut self, peer_addr: SocketAddr, msg: Bytes) -> Result<()> {
        if self.response_from_bootstrap_node(&peer_addr) {
            let msg: Rpc = bincode::deserialize(&msg)?;
            match msg {
                Rpc::StartTest(peers) => self.connect_to_peers(peers).await?,
            }
        } else {
            let small_msg_rcvd = self.peer_states.entry(peer_addr).or_insert(false);

            debug!("[{}] Message received: {}", peer_addr, msg.len());
            assert!(hash_correct(&msg));

            let payload_size = msg.len() - 4; // without the hash

            if payload_size == LARGE_MSG_SIZE {
                if !*small_msg_rcvd {
                    error!("[{}] Large message received before small", peer_addr);
                } else {
                    self.received_messages += 1;
                    debug!(
                        "Recv: {}/{}, Sent: {}/{}",
                        self.received_messages,
                        self.client_nodes.len(),
                        self.sent_messages,
                        self.client_nodes.len()
                    );

                    if self.received_messages == self.client_nodes.len()
                        && self.sent_messages == self.client_nodes.len()
                    {
                        info!("Done. All checks passed");
                    }
                }
            } else if payload_size == SMALL_MSG_SIZE {
                if *small_msg_rcvd {
                    error!("[{}] Small message received twice", peer_addr);
                }
                *small_msg_rcvd = true;
            }
        }
        Ok(())
    }

    async fn connect_to_peers(&mut self, peers: Vec<SocketAddr>) -> Result<()> {
        for peer in peers {
            if peer != self.our_addr {
                self.endpoint.connect_to(&peer).await?;
                self.client_nodes.insert(peer);
            }
        }
        Ok(())
    }

    fn response_from_bootstrap_node(&self, peer_addr: &SocketAddr) -> bool {
        peer_addr == &self.bootstrap_node_addr
    }
}

fn random_data_with_hash(size: usize) -> Vec<u8> {
    let mut data = random_vec(size + 4);
    let hash = crc32::checksum_ieee(&data[4..]);
    // write hash in big endian
    data[0] = (hash >> 24) as u8;
    data[1] = ((hash >> 16) & 0xff) as u8;
    data[2] = ((hash >> 8) & 0xff) as u8;
    data[3] = (hash & 0xff) as u8;
    data
}

fn hash_correct(data: &[u8]) -> bool {
    let encoded_hash = (u32::from(data[0]) << 24)
        | (u32::from(data[1]) << 16)
        | (u32::from(data[2]) << 8)
        | u32::from(data[3]);
    let actual_hash = crc32::checksum_ieee(&data[4..]);
    encoded_hash == actual_hash
}

fn random_vec(size: usize) -> Vec<u8> {
    rand::thread_rng()
        .sample_iter(&Standard)
        .take(size)
        .collect()
}
