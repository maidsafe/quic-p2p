// Copyright 2019 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// http://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use crate::{
    error::{Error, Result},
    utils,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fmt,
    net::{IpAddr, SocketAddr},
    str::FromStr,
};
use structopt::StructOpt;

/// QuicP2p configurations
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq, StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct Config {
    /// Hard Coded contacts
    #[structopt(
        short,
        long,
        default_value = "[]",
        parse(try_from_str = serde_json::from_str)
    )]
    pub hard_coded_contacts: HashSet<SocketAddr>,
    /// Port we want to reserve for QUIC. If none supplied we'll use the OS given random port.
    /// If external port is provided it means that the user is carrying out manual port forwarding and this field is mandatory.
    /// This will be the internal port number mapped to the process
    #[structopt(long)]
    pub local_port: Option<u16>,
    /// IP address for the listener. If none is supplied and `forward_port` is enabled, we will use IGD to realize the
    /// local IP address of the machine. If IGD fails the application will exit.
    #[structopt(long)]
    pub local_ip: Option<IpAddr>,
    /// Specify if port forwarding via UPnP should be done or not. This can be set to false if the network
    /// is run locally on the network loopback or on a local area network.
    #[structopt(long)]
    pub use_igd: bool,
    /// External port number assigned to the socket address of the program.
    /// If this is provided, QP2p considers that the local port provided has been mapped to the
    /// provided external port number and automatic port forwarding will be skipped.
    #[structopt(long)]
    pub external_port: Option<u16>,
    /// External IP address of the computer on the WAN. This field is mandatory if the node is the genesis node and
    /// port forwarding is not available. In case of non-genesis nodes, the external IP address will be resolved
    /// using the Echo service.
    #[structopt(long)]
    pub external_ip: Option<IpAddr>,
    /// This is the maximum message size we'll allow the peer to send to us. Any bigger message and
    /// we'll error out probably shutting down the connection to the peer. If none supplied we'll
    /// default to the documented constant.
    #[structopt(long)]
    pub max_msg_size_allowed: Option<u32>,
    /// If we hear nothing from the peer in the given interval we declare it offline to us. If none
    /// supplied we'll default to the documented constant.
    ///
    /// The interval is in milliseconds. A value of 0 disables this feature.
    #[structopt(long)]
    pub idle_timeout_msec: Option<u64>,
    /// Interval to send keep-alives if we are idling so that the peer does not disconnect from us
    /// declaring us offline. If none is supplied we'll default to the documented constant.
    ///
    /// The interval is in milliseconds. A value of 0 disables this feature.
    #[structopt(long)]
    pub keep_alive_interval_msec: Option<u32>,
    /// Directory in which the bootstrap cache will be stored. If none is supplied, the platform specific
    /// default cache directory is used.
    #[structopt(long)]
    pub bootstrap_cache_dir: Option<String>,
    /// Duration of a UPnP port mapping.
    #[structopt(long)]
    pub upnp_lease_duration: Option<u32>,
}

/// To be used to read and write our certificate and private key to disk esp. as a part of our
/// configuration file
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub(crate) struct SerialisableCertificate {
    /// DER encoded certificate
    pub cert_der: Bytes,
    /// DER encoded private key
    pub key_der: Bytes,
}

impl SerialisableCertificate {
    /// Returns a new Certificate that is valid for the list of domain names provided
    pub fn new(domains: impl Into<Vec<String>>) -> Result<Self> {
        let cert = rcgen::generate_simple_self_signed(domains)?;
        Ok(Self {
            cert_der: cert.serialize_der()?.into(),
            key_der: cert.serialize_private_key_der().into(),
        })
    }

    /// Parses DER encoded binary key material to a format that can be used by Quinn
    ///
    /// # Errors
    /// Returns [CertificateParseError](Error::CertificateParseError) if the inputs
    /// cannot be parsed
    pub fn obtain_priv_key_and_cert(&self) -> Result<(quinn::PrivateKey, quinn::Certificate)> {
        Ok((
            quinn::PrivateKey::from_der(&self.key_der).map_err(|_| Error::CertificatePkParse)?,
            quinn::Certificate::from_der(&self.cert_der).map_err(|_| Error::CertificateParse)?,
        ))
    }
}

impl FromStr for SerialisableCertificate {
    type Err = Error;

    /// Decode `SerialisableCertificate` from a base64-encoded string.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let cert = base64::decode(s)?;
        let (cert_der, key_der) = bincode::deserialize(&cert)?;
        Ok(Self { cert_der, key_der })
    }
}

impl fmt::Debug for SerialisableCertificate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SerialisableCertificate {{ cert_der: {}, key_der: <HIDDEN> }}",
            utils::bin_data_format(&self.cert_der)
        )
    }
}
