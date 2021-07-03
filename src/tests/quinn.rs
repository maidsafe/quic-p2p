use std::net::SocketAddr;

use super::{hash, random_msg};
use crate::utils;
use crate::{api::bind, config::SerialisableCertificate, peer_config};
use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use log::{error, trace, warn};
use peer_config::{DEFAULT_IDLE_TIMEOUT_MSEC, DEFAULT_KEEP_ALIVE_INTERVAL_MSEC};
use std::collections::BTreeSet;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

const DOMAIN: &str = "quinn.test";

#[derive(Clone)]
struct Peer {
    endpoint: quinn::Endpoint,
    client_cfg: quinn::ClientConfig,
    message_tx: UnboundedSender<(SocketAddr, Bytes)>,
}

impl Peer {
    fn new() -> Result<(Self, UnboundedReceiver<(SocketAddr, Bytes)>)> {
        let (key, cert) = {
            let our_complete_cert = SerialisableCertificate::new(vec![DOMAIN.to_string()])?;
            our_complete_cert.obtain_priv_key_and_cert()?
        };

        let endpoint_cfg = peer_config::new_our_cfg(
            DEFAULT_IDLE_TIMEOUT_MSEC,
            DEFAULT_KEEP_ALIVE_INTERVAL_MSEC,
            cert,
            key,
        )?;

        let client_cfg = peer_config::new_client_cfg(
            DEFAULT_IDLE_TIMEOUT_MSEC,
            DEFAULT_KEEP_ALIVE_INTERVAL_MSEC,
        )?;

        let (endpoint, mut incoming) = bind(endpoint_cfg, "127.0.0.1:0".parse()?, true)?;

        let (message_tx, message_rx) = unbounded_channel();
        let message_sender = message_tx.clone();
        let _ = tokio::spawn(async move {
            loop {
                match incoming.next().await {
                    Some(quinn_conn) => match quinn_conn.await {
                        Ok(quinn::NewConnection {
                            connection,
                            uni_streams,
                            ..
                        }) => {
                            let peer_address = connection.remote_address();
                            listen_for_messages(uni_streams, peer_address, message_sender.clone());
                        }
                        Err(err) => {
                            error!(
                                "An incoming connection failed because of an error: {:?}",
                                err
                            );
                        }
                    },
                    None => {
                        trace!("quinn::Incoming::next() returned None. There will be no more incoming connections");
                        break;
                    }
                }
            }
        });

        Ok((
            Peer {
                endpoint,
                client_cfg,
                message_tx,
            },
            message_rx,
        ))
    }

    fn socket_addr(&self) -> Result<SocketAddr> {
        Ok(self.endpoint.local_addr()?)
    }

    async fn send_message(&self, addr: &SocketAddr, msg: Bytes) -> Result<()> {
        let new_conn = self
            .endpoint
            .connect_with(self.client_cfg.clone(), &addr, DOMAIN)?
            .await?;
        let mut stream = new_conn.connection.open_uni().await?;
        let _ = stream.write(&msg).await?;
        stream.finish().await?;
        Ok(())
    }
}

fn listen_for_messages(
    mut uni_streams: quinn::IncomingUniStreams,
    src: SocketAddr,
    message_tx: UnboundedSender<(SocketAddr, Bytes)>,
) {
    let _: JoinHandle<Result<()>> = tokio::spawn(async move {
        read_from_stream(&mut uni_streams, src, message_tx.clone()).await?;
        log::trace!("The connection to {:?} has been terminated.", src);
        Ok::<(), anyhow::Error>(())
    });
}

async fn read_from_stream(
    uni_streams: &mut quinn::IncomingUniStreams,
    peer_addr: SocketAddr,
    message_tx: UnboundedSender<(SocketAddr, Bytes)>,
) -> Result<()> {
    while let Some(result) = uni_streams.next().await {
        match result {
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                trace!("Connection terminated by peer {:?}.", peer_addr);
                break;
            }
            Err(err) => {
                warn!(
                    "Failed to read incoming message on uni-stream for peer {:?} with error: {:?}",
                    peer_addr, err
                );
                break;
            }
            Ok(recv) => {
                let bytes = recv.read_to_end(1024).await?;
                let _ = message_tx.send((peer_addr, Bytes::from(bytes)));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn multiple_connections_with_many_concurrent_messages() -> Result<()> {
    use futures::future;

    utils::init_logging();

    let num_senders: usize = 10;
    let num_messages_each: usize = 100;
    let num_messages_total: usize = 1000;

    let (server_endpoint, mut recv_incoming_messages) = Peer::new()?;
    let server_addr = server_endpoint.socket_addr()?;

    let test_msgs: Vec<_> = (0..num_messages_each).map(|_| random_msg(1024)).collect();
    let sending_msgs = test_msgs.clone();

    let mut tasks = Vec::new();

    // let message_sender = message_tx.clone();
    // Receiver
    tasks.push(tokio::spawn(async move {
        let mut num_received = 0;
        let mut sending_tasks = Vec::new();

        while let Some((src, msg)) = recv_incoming_messages.recv().await {
            log::info!("received from {:?} with message size {}", src, msg.len());
            assert_eq!(msg.len(), test_msgs[0].len());

            let sending_endpoint = server_endpoint.clone();

            sending_tasks.push(tokio::spawn({
                async move {
                    // Hash the inputs for couple times to simulate certain workload.
                    let hash_result = hash(&msg);
                    for _ in 0..5 {
                        let _ = hash(&msg);
                    }
                    sending_endpoint
                        .send_message(&src, hash_result.to_vec().into())
                        .await?;

                    Ok::<_, anyhow::Error>(())
                }
            }));

            num_received += 1;
            if num_received >= num_messages_total {
                break;
            }
        }

        let _ = future::try_join_all(sending_tasks).await?;

        Ok(())
    }));

    // Sender
    for id in 0..num_senders {
        let messages = sending_msgs.clone();
        tasks.push(tokio::spawn({
            let (send_endpoint, mut recv_incoming_messages) = Peer::new()?;

            async move {
                let mut hash_results = BTreeSet::new();
                log::info!("connecting {}", id);
                for (index, message) in messages.iter().enumerate().take(num_messages_each) {
                    let _ = hash_results.insert(hash(&message));
                    log::info!("sender #{} sending message #{}", id, index);
                    send_endpoint
                        .send_message(&server_addr, message.clone())
                        .await?;
                }

                log::info!(
                    "sender #{} completed sending messages, starts listening",
                    id
                );

                while let Some((src, msg)) = recv_incoming_messages.recv().await {
                    log::info!(
                        "#{} received from server {:?} with message size {}",
                        id,
                        src,
                        msg.len()
                    );
                    assert!(hash_results.remove(&msg[..]));
                    if hash_results.is_empty() {
                        break;
                    }
                }

                Ok::<_, anyhow::Error>(())
            }
        }));
    }

    let _ = future::try_join_all(tasks).await?;
    Ok(())
}
