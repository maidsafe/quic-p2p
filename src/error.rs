// Copyright 2019 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use super::wire_msg::WireMsg;
use std::io;
use thiserror::Error;

/// Result used by `QuicP2p`.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error types returned by the qp2p public API.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Error joining a thread
    #[error("Problem spawning thread")]
    ThreadError,
    /// Error occurred when attempting to connect to any
    /// of the peers provided as a list of contacts.
    #[error("Network bootstrap failed")]
    BootstrapFailure,
    /// No peers/contacts found in the bootstrap nodes list.
    #[error("No nodes/peers found defined for bootstrapping")]
    EmptyBootstrapNodesList,
    /// The path provided is not valid for the operation.
    #[error("Provided path is invalid: {0}")]
    InvalidPath(String),
    /// The user's home directory couldn't be determined.
    #[error("Couldn't determine user's home directory")]
    UserHomeDir,
    /// I/O failure when attempting to access a local resource.
    #[error("I/O Error")]
    Io(#[from] io::Error),
    /// Failure encountered when establishing a connection with another peer.
    #[error("Establishing connection")]
    Connect(#[from] quinn::ConnectError),
    /// An existing connection with another peer has been lost.
    #[error("Connection lost")]
    Connection(#[from] quinn::ConnectionError),
    /// Failed to create a new endpoint.
    #[error("Creating endpoint")]
    Endpoint(#[from] quinn::EndpointError),
    /// Certificate for secure communication couldn't be parsed.
    #[error("Cannot parse certificate ")]
    CertificateParse,
    /// The certificate's private key for secure communication couldn't be parsed.
    #[error("Cannot parse certificate's private key")]
    CertificatePkParse,
    /// The contacts list was found empty when attempting
    /// to contact peers for the echo service.
    #[error("No peers found in the contacts list to send the echo request to")]
    NoEchoServerEndpointDefined,
    /// Timeout occurred when awaiting for a response from
    /// any of the peers contacted for the echo service.
    #[error("No response received from echo services")]
    NoEchoServiceResponse,
    /// Failure occurred when sending an echo request.
    #[error("{0}")]
    EchoServiceFailure(String),
    /// TLS error
    #[error("TLS Error ")]
    Tls(#[from] rustls::TLSError),
    /// Serialisation error, which can happen on different type of data.
    #[error("Serialisation error")]
    Serialisation(#[from] bincode::Error),
    /// Failed to decode a base64-encoded string.
    #[error("Base64 decode")]
    Base64Decode(#[from] base64::DecodeError),
    /// Cannot assign port to endpoint
    #[error("Cannot assign port to endpoint {0}")]
    CannotAssignPort(u16),
    /// An error occurred which could be resolved by changing some config value.
    #[error("Configuration {0}")]
    Configuration(String),
    /// The message type flag decoded in an incoming stream is invalid/unsupported.
    #[error("Invalid message type flag found in message's header: {0}")]
    InvalidMsgFlag(u8),
    /// Error occurred when trying to write on a currently opened message stream.
    #[error("Stream write error")]
    StreamWrite(#[from] quinn::WriteError),
    /// The expected amount of message bytes couldn't be read from the stream.
    #[error("Failed to read expected number of message bytes: {0}")]
    StreamRead(#[from] quinn::ReadExactError),
    /// Failure when trying to map a new port using IGD for automatic port forwarding.
    #[error("Could not use IGD for automatic port forwarding")]
    IgdAddPort(#[from] igd::AddAnyPortError),
    /// Failure when trying to renew leasing of a port mapped using IGD.
    #[error("Could not renew port mapping using IGD")]
    IgdRenewPort(#[from] igd::AddPortError),
    /// IGD gateway deice was not found.
    #[error("Could not find the gateway device for IGD")]
    IgdSearch(#[from] igd::SearchError),
    /// IGD is not supported on IPv6
    #[error("IGD is not supported on IPv6")]
    IgdNotSupported,
    /// An error was encountered when trying to either generate
    /// or serialise a self-signed certificate.
    #[error("Self-signed certificate generation error: {0}")]
    CertificateGen(#[from] rcgen::RcgenError),
    /// Response message received contains an empty payload.
    #[error("Empty response message received from peer")]
    EmptyResponse,
    /// The type of message received is not the expected one.
    #[error("Type of the message received was not the expected one: {0}")]
    UnexpectedMessageType(WireMsg),
    /// The message exceeds the maximum message length allowed.
    #[error("Maximum data length exceeded, length: {0}")]
    MaxLengthExceeded(usize),
    /// Incorrect Public Address provided
    #[error("Incorrect Public Address provided")]
    IncorrectPublicAddress,
    /// Missing connection
    #[error("No connection to the dest peer")]
    MissingConnection,
    /// A supposedly impossible internal error occurred
    #[error("Unexpected internal error: {0}")]
    UnexpectedError(String),
    /// Unspecified local IP address
    #[error("Unspecified Local IP address")]
    UnspecifiedLocalIp,
    /// Couldn't resolve Public IP address
    #[error("Unresolved Public IP address")]
    UnresolvedPublicIp,
}
