// Copyright 2019 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

//! Bootstrap node which acts as a relay for other client nodes. It collects the info of multiple
//! client nodes and relays it to all remaining connected nodes, hence allows them all to connect
//! with each other.
//!
//! Usage:
//! ```
//! $ RUST_LOG=bootstrap_node=info cargo run --example bootstrap_node -- --expected_conns 1 --ip
//! "127.0.0.1"
//! ```

mod common;

use anyhow::Result;
use bytes::Bytes;
use common::{Event, EventReceivers, Rpc};
use log::{error, info, warn};
use qp2p::{Config, QuicP2p};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use structopt::StructOpt;
use tracing_subscriber::EnvFilter;

/// Configuration for the bootstrap node
#[derive(Serialize, Deserialize, StructOpt)]
pub struct BootstrapNodeConfig {
    /// A number of expected connections.
    /// Once this number is reached, we'll send a list of all connections to every connected peer.
    #[structopt(short, long)]
    expected_conns: usize,
    #[structopt(flatten)]
    quic_p2p_opts: Config,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Initialise configuration
    let bootstrap_node_config = BootstrapNodeConfig::from_args();

    // Initialise QuicP2p

    let qp2p = QuicP2p::with_config(
        Some(bootstrap_node_config.quic_p2p_opts),
        Default::default(),
        false,
    )?;
    let (endpoint, incoming_connections, incoming_messages, disconnections) =
        qp2p.new_endpoint().await?;
    let mut event_rx = EventReceivers {
        incoming_connections,
        incoming_messages,
        disconnections,
    };

    let our_addr = endpoint.socket_addr();
    info!("Endpoint started on {}", our_addr);

    println!("Our connection info: {}", our_addr);

    let expected_connections = bootstrap_node_config.expected_conns;
    let mut connected_peers = HashMap::new();
    let mut test_triggered = false;

    while let Some(event) = event_rx.recv().await {
        match event {
            Event::ConnectedTo { addr } => {
                let _ = connected_peers.insert(addr, addr);
                if connected_peers.len() == expected_connections && !test_triggered {
                    info!(
                        "{} connections collected, triggering the test",
                        expected_connections
                    );
                    let contacts: Vec<_> = connected_peers.values().cloned().collect();
                    let msg = Bytes::from(bincode::serialize(&Rpc::StartTest(contacts))?);
                    for peer in connected_peers.values() {
                        endpoint.send_message(msg.clone(), peer).await?;
                    }
                    test_triggered = true;
                } else if connected_peers.len() >= expected_connections {
                    error!("More than expected connections received");
                }
            }
            event => warn!("Unexpected event: {:?}", event),
        }
    }

    Ok(())
}
