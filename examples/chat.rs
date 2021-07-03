// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

//! Basic chat like example that demonstrates how to connect with peers and exchange data.

mod common;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use common::{Event, EventReceivers};
use qp2p::{Config, Endpoint, QuicP2p};
use rand::{distributions::Standard, Rng};
use rustyline::config::Configurer;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use structopt::StructOpt;
use tokio::task::JoinHandle;

struct PeerList {
    peers: Vec<SocketAddr>,
}

impl PeerList {
    fn new() -> Self {
        Self { peers: Vec::new() }
    }

    fn insert_from_str(&mut self, input: &str) -> Result<()> {
        parse_peer(input).map(|v| self.insert(v))
    }

    fn insert(&mut self, peer: SocketAddr) {
        if !self.peers.contains(&peer) {
            self.peers.push(peer)
        }
    }

    fn remove(&mut self, peer_idx: usize) -> Result<SocketAddr> {
        if peer_idx < self.peers.len() {
            Ok(self.peers.remove(peer_idx))
        } else {
            Err(anyhow!("Index out of bounds"))
        }
    }

    fn get(&self, peer_idx: usize) -> Option<&SocketAddr> {
        self.peers.get(peer_idx)
    }

    fn list(&self) {
        for (idx, peer) in self.peers.iter().enumerate() {
            println!("{:3}: client:{}", idx, peer);
        }
    }
}

/// This chat app connects two machines directly without intermediate servers and allows
/// to exchange messages securely. All the messages are end to end encrypted.
#[derive(Debug, StructOpt)]
struct CliArgs {
    #[structopt(flatten)]
    quic_p2p_opts: Config,
}

#[tokio::main]
async fn main() -> Result<()> {
    let CliArgs { quic_p2p_opts } = CliArgs::from_args();

    let qp2p = QuicP2p::with_config(Some(quic_p2p_opts), Default::default(), false)?;
    let (endpoint, incoming_connections, incoming_messages, disconnections) =
        qp2p.new_endpoint().await?;
    let event_rx = EventReceivers {
        incoming_connections,
        incoming_messages,
        disconnections,
    };

    print_logo();
    println!("Type 'help' to get started.");

    let peerlist = Arc::new(Mutex::new(PeerList::new()));
    let rx_thread = handle_qp2p_events(event_rx, peerlist.clone());

    let mut rl = Editor::<()>::new();
    rl.set_auto_add_history(true);
    'outer: loop {
        match rl.readline(">> ") {
            Ok(line) => {
                let mut args = line.trim().split_whitespace();
                let cmd = if let Some(cmd) = args.next() {
                    cmd
                } else {
                    continue 'outer;
                };
                let mut peerlist = peerlist.lock().unwrap();
                let result = match cmd {
                    "ourinfo" => {
                        print_ourinfo(&endpoint);
                        Ok(())
                    }
                    "addpeer" => peerlist.insert_from_str(&args.collect::<Vec<_>>().join(" ")),
                    "listpeers" => {
                        peerlist.list();
                        Ok(())
                    }
                    "delpeer" => args
                        .next()
                        .ok_or_else(|| anyhow!("Missing index argument"))
                        .and_then(|idx| idx.parse().map_err(|_| anyhow!("Invalid index argument")))
                        .and_then(|idx| peerlist.remove(idx))
                        .and(Ok(())),
                    "send" => on_cmd_send(&mut args, &peerlist, &endpoint).await,
                    "sendrand" => on_cmd_send_rand(&mut args, &peerlist, &endpoint).await,
                    "quit" | "exit" => break 'outer,
                    "help" => {
                        println!(
                            "Commands: ourinfo, addpeer, listpeers, delpeer, send, quit, exit, help"
                        );
                        Ok(())
                    }
                    _ => Err(anyhow!("Unknown command")),
                };
                if let Err(msg) = result {
                    println!("Error: {}", msg);
                }
            }
            Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => break 'outer,
            Err(e) => {
                println!("Error reading line: {}", e);
            }
        }
    }

    drop(qp2p);
    rx_thread.await?;
    Ok(())
}

async fn on_cmd_send<'a>(
    mut args: impl Iterator<Item = &'a str>,
    peer_list: &PeerList,
    endpoint: &Endpoint,
) -> Result<()> {
    let peer = args
        .next()
        .with_context(|| "Missing index argument")
        .and_then(|idx| idx.parse().map_err(|_| anyhow!("Invalid index argument")))
        .and_then(|idx| {
            peer_list
                .get(idx)
                .ok_or_else(|| anyhow!("Index out of bounds"))
        })?;
    let msg = Bytes::from(args.collect::<Vec<_>>().join(" "));
    endpoint.send_message(msg, peer).await.map_err(From::from)
}

/// Sends random data of given size to given peer.
/// Usage: "sendrand <peer_index> <bytes>
async fn on_cmd_send_rand<'a>(
    mut args: impl Iterator<Item = &'a str>,
    peer_list: &PeerList,
    endpoint: &Endpoint,
) -> Result<()> {
    let (addr, msg_len) = args
        .next()
        .ok_or_else(|| anyhow!("Missing index argument"))
        .and_then(|idx| idx.parse().map_err(|_| anyhow!("Invalid index argument")))
        .and_then(|idx| {
            peer_list
                .get(idx)
                .ok_or_else(|| anyhow!("Index out of bounds"))
        })
        .and_then(|peer| {
            args.next()
                .ok_or_else(|| anyhow!("Missing bytes count"))
                .and_then(|bytes| {
                    bytes
                        .parse::<usize>()
                        .map_err(|_| anyhow!("Invalid bytes count argument"))
                })
                .map(|bytes_to_send| (peer, bytes_to_send))
        })?;
    let data = Bytes::from(random_vec(msg_len));
    endpoint.send_message(data, addr).await.map_err(From::from)
}

fn handle_qp2p_events(
    mut event_rx: EventReceivers,
    peer_list: Arc<Mutex<PeerList>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                Event::ConnectedTo { addr } => peer_list.lock().unwrap().insert(addr),
                Event::NewMessage { src, msg } => {
                    if msg.len() > 512 {
                        println!("[{}] received bytes: {}", src, msg.len());
                    } else {
                        println!(
                            "[{}] {}",
                            src,
                            String::from_utf8(msg.to_vec())
                                .unwrap_or_else(|_| "Invalid String".to_string())
                        );
                    }
                } // event => println!("Unexpected quic-p2p event: {:?}", event),
            }
        }
    })
}

fn parse_peer(input: &str) -> Result<SocketAddr> {
    parse_socket_addr(&input).map_err(|_| {
        anyhow!("Invalid peer (valid examples: \"1.2.3.4:5678\", \"8.7.6.5:4321\", ...)")
    })
}

fn parse_socket_addr(input: &str) -> Result<SocketAddr> {
    input.parse().map_err(|_| anyhow!("Invalid socket address"))
}

fn print_ourinfo(endpoint: &Endpoint) {
    let ourinfo = endpoint.socket_addr();

    println!("Our info: {}", ourinfo);
}

fn print_logo() {
    println!(
        r#"
             ____                _           _
  __ _  _ __|___ \ _ __      ___| |__   __ _| |_
 / _` || '_ \ __) | '_ \    / __| '_ \ / _` | __|
| (_| || |_) / __/| |_) |  | (__| | | | (_| | |_
 \__, || .__/_____| .__/    \___|_| |_|\__,_|\__|
    |_||_|        |_|
  "#
    );
}

fn random_vec(size: usize) -> Vec<u8> {
    rand::thread_rng()
        .sample_iter(&Standard)
        .take(size)
        .collect()
}
