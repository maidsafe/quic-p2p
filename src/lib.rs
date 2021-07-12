// Copyright 2019 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

//! qp2p enables communication within a peer to peer network over the QUIC protocol.

// For explanation of lint checks, run `rustc -W help`
#![forbid(
    arithmetic_overflow,
    mutable_transmutes,
    no_mangle_const_items,
    unknown_crate_types
)]
#![deny(
    bad_style,
    deprecated,
    improper_ctypes,
    missing_docs,
    non_shorthand_field_patterns,
    overflowing_literals,
    stable_features,
    unconditional_recursion,
    unknown_lints,
    unsafe_code,
    unused_allocation,
    unused_attributes,
    unused_comparisons,
    unused_features,
    unused_parens,
    while_true,
    clippy::unicode_not_nfc,
    clippy::wrong_pub_self_convention,
    warnings
)]
#![warn(
    trivial_casts,
    trivial_numeric_casts,
    unused,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results
)]

mod api;
mod bootstrap_cache;
mod config;
mod connection_deduplicator;
mod connection_pool;
mod connections;
mod endpoint;
mod error;
#[cfg(not(feature = "no-igd"))]
mod igd;
mod peer_config;
#[cfg(test)]
mod test_utils;
mod utils;
mod wire_msg;

pub use api::QuicP2p;
pub use config::Config;
pub use connections::{DisconnectionEvents, RecvStream, SendStream};
pub use endpoint::{Endpoint, IncomingConnections, IncomingMessages};
pub use error::{Error, Result};
pub use quinn::ConnectionError;

#[cfg(test)]
mod tests;
