// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use std::net::SocketAddr;

// use anyhow::{anyhow, Result};
use bytes::Bytes;
use crossbeam_channel as mpmc;
use futures::{select, FutureExt};
use qp2p::{DisconnectionEvents, IncomingConnections, IncomingMessages};
use serde::{Deserialize, Serialize};

/// Remote procedure call for our examples to communicate.
#[derive(Debug, Serialize, Deserialize)]
pub enum Rpc {
    /// Starts the connectivity and data exchange test between us and given QuicP2p peers.
    StartTest(Vec<SocketAddr>),
}

#[derive(Debug)]
pub enum Event {
    NewMessage { src: SocketAddr, msg: Bytes },
    ConnectedTo { addr: SocketAddr },
}

/// Receivers side for events
pub struct EventReceivers {
    pub incoming_messages: IncomingMessages,
    pub incoming_connections: IncomingConnections,
    pub disconnections: DisconnectionEvents,
}

#[allow(unused)]
pub struct EventSenders {
    pub node_tx: mpmc::Sender<Event>,
    pub client_tx: mpmc::Sender<Event>,
}

impl EventReceivers {
    #[allow(unused)]
    pub async fn recv(&mut self) -> Option<Event> {
        select! {
            incoming_message = self.incoming_messages.next().fuse() => {
                if let Some((src, msg)) = incoming_message {
                    Some(Event::NewMessage { src, msg })
                } else {
                    None
                }
            },
            addr = self.incoming_connections.next().fuse() => {
                if let Some(addr) = addr {
                    Some(Event::ConnectedTo { addr })
                } else {
                    None
                }
            }
        }
    }

    // #[allow(unused)]
    // pub fn iter(&self) -> IterEvent {
    //     IterEvent { event_rx: &self }
    // }
}

// pub struct IterEvent<'a> {
//     event_rx: &'a EventReceivers,
// }

// impl<'a> Iterator for IterEvent<'a> {
//     type Item = Event;

//     fn next(&mut self) -> Option<Self::Item> {
//         let mut sel = mpmc::Select::new();
//         let message_idx = sel.recv(&self.event_rx.incoming_messages);
//         let connection_idx = sel.recv(&self.event_rx.incoming_connections);
//         let selected_operation = sel.ready();

//         let event = if selected_operation == client_idx {
//             self.event_rx.client_rx.recv()
//         } else if selected_operation == node_idx {
//             self.event_rx.node_rx.recv()
//         } else {
//             return None;
//         };

//         event.ok()
//     }
// }
