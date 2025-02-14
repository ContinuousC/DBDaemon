/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::net;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::warn;
use rustls::ClientConfig;
use serde::{Deserialize, Serialize};
use serde_cbor::Value;
use thiserror::Error;
use tokio::{
    fs,
    net::TcpStream,
    sync::{mpsc, watch},
};
use tokio_rustls::{client::TlsStream, rustls};
use webpki::{DNSName, DNSNameRef};

use rpc::{AsyncDuplex, CborStream, MsgStream};

use crate::daemon::DbDaemon;
use crate::AgentDbService;

pub type Request<Value> = AsyncDuplex<Value, Void, Void>;
pub type Response<Value> =
    AsyncDuplex<Void, std::result::Result<Value, String>, Void>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Void {}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(pub String);

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum BrokerToDatabaseMessage<Value> {
    Agent {
        agent_id: AgentId,
        message: Request<Value>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseToBrokerMessage<Value> {
    Agent {
        agent_id: AgentId,
        message: Response<Value>,
    },
}

pub struct AgentConnector {
    tls_config: Arc<ClientConfig>,
    sock_addr: SocketAddr,
    dns_name: DNSName,
}

impl AgentConnector {
    pub async fn new(
        sock_addr: SocketAddr,
        domain: String,
        certs_path: &Path,
    ) -> Result<Self, AgentError> {
        let dns_name = DNSNameRef::try_from_ascii_str(&domain)?.to_owned();
        let tls_config = tls_config(certs_path).await?;

        Ok(Self {
            tls_config,
            sock_addr,
            dns_name,
        })
    }

    pub async fn connect(
        &self,
    ) -> Result<CborStream<TlsStream<TcpStream>>, AgentError> {
        let tcp_stream = TcpStream::connect(self.sock_addr)
            .await
            .map_err(AgentError::Connect)?;
        let tls_stream =
            tokio_rustls::TlsConnector::from(self.tls_config.clone())
                .connect(self.dns_name.as_ref(), tcp_stream)
                .await
                .map_err(AgentError::TlsConnect)?;

        let mut stream = CborStream::new(tls_stream);
        rpc::handshake_client(&mut stream, 0).await?;
        Ok(stream)
    }
}

pub async fn handle_conn<
    S: MsgStream<DatabaseToBrokerMessage<Value>, BrokerToDatabaseMessage<Value>>,
>(
    mut stream: S,
    service: Arc<DbDaemon>,
    mut term: watch::Receiver<bool>,
) {
    let mut eof = false;
    let (tx, mut rx) = mpsc::channel(100);
    loop {
        tokio::select! {
            req = stream.recv(), if !eof => {
                match req {
                    Ok(req) => match tx.clone().reserve_owned().await {
                        Ok(permit) => {
                            tokio::spawn(handle_request(req, permit, service.clone()));
                        },
                        Err(_) => match req {
                            BrokerToDatabaseMessage::Agent { agent_id, message } => {
                                if let AsyncDuplex::Request { req_id, request: _ } = message {
                                    let res = DatabaseToBrokerMessage::Agent {
                                        agent_id,
                                        message: AsyncDuplex::Response {
                                            req_id,
                                            response: Err("shutdown in progress".to_string())
                                        }
                                    };
                                    if let Err(e) = stream.send(res).await {
                                        println!("Error sending response over socket: {:?}", e);
                                    }
                                }
                            }
                        }
                    },
                    Err(err) => match err {
                        rpc::Error::Cbor(err) => {
                            warn!("Failed to decode agent request (invalid CBOR): {}", err);
                        }
                        rpc::Error::Eof => { rx.close(); eof = true; },
                        _ => { warn!("Receive error on socket: {:?}", err); break; }
                    }
                }
            }
            res = rx.recv() => {
                if let Some(res) = res {
                    if let Err(e) = stream.send(res).await {
                        println!("Error sending response over socket: {:?}", e)
                    }
                } else {
                    /* Shutdown in progress and all messages handled. */
                    break;
                }
            }
            r = term.changed() => match r {
                Ok(()) => if *term.borrow() { rx.close() },
                Err(e) => {
                    warn!("Term signal channel failed: {}", e);
                    rx.close();
                }
            }
        };
    }
}

async fn handle_request(
    req: BrokerToDatabaseMessage<Value>,
    tx: mpsc::OwnedPermit<DatabaseToBrokerMessage<Value>>,
    service: Arc<DbDaemon>,
) {
    match req {
        BrokerToDatabaseMessage::Agent { agent_id, message } => {
            if let AsyncDuplex::Request { req_id, request } = message {
                tx.send(DatabaseToBrokerMessage::Agent {
                    agent_id,
                    message: AsyncDuplex::Response {
                        req_id,
                        response: match serde_cbor::value::from_value(request) {
                            Ok(req) => service.request(req).await,
                            Err(e) => {
                                Err(format!("Failed to decode request: {}", e))
                            }
                        },
                    },
                });
            }
        }
    }
}

async fn tls_config(
    certs_path: &Path,
) -> Result<Arc<ClientConfig>, AgentError> {
    let mut tls_config = rustls::ClientConfig::new();

    let ca_cert = certs_path.join("ca.crt");
    let ca_file = fs::read(&ca_cert).await.map_err(AgentError::ReadCert)?;
    tls_config
        .root_store
        .add_pem_file(&mut &ca_file[..])
        .map_err(|_| AgentError::InvalidPemFile(PathBuf::from(&ca_cert)))?;

    let daemon_key = certs_path.join("dbdaemon.key");
    let key_file = fs::read(&daemon_key).await.map_err(AgentError::ReadKey)?;
    let key = rustls::internal::pemfile::pkcs8_private_keys(&mut &key_file[..])
        .map_err(|_| AgentError::InvalidPemFile(PathBuf::from(&daemon_key)))?
        .into_iter()
        .next()
        .ok_or_else(|| {
            AgentError::InvalidPemFile(PathBuf::from(&daemon_key))
        })?;

    let daemon_cert = certs_path.join("dbdaemon.crt");
    let mut certs = Vec::new();
    for path in &[daemon_cert, ca_cert] {
        let cert_file = fs::read(path).await.map_err(AgentError::ReadCert)?;
        certs.extend(
            rustls::internal::pemfile::certs(&mut &cert_file[..])
                .map_err(|_| AgentError::InvalidPemFile(PathBuf::from(path)))?,
        );
    }
    tls_config.set_single_client_cert(certs, key)?;
    Ok(Arc::new(tls_config))
}

#[derive(Error, Debug)]
pub enum AgentError {
    #[error("RPC error: {0}")]
    RPCError(#[from] rpc::Error),
    #[error("TLS error: {0}")]
    TLSError(#[from] rustls::TLSError),
    #[error("Failed to read certificate: {0}")]
    ReadCert(std::io::Error),
    #[error("Failed to read key: {0}")]
    ReadKey(std::io::Error),
    #[error("TCP connect failed: {0}")]
    Connect(std::io::Error),
    #[error("TLS connect failed: {0}")]
    TlsConnect(std::io::Error),
    #[error("Invalid dns name: {0}")]
    InvalidDnsName(#[from] webpki::InvalidDNSNameError),
    #[error("Invalid address: {0}")]
    InvalidAddr(#[from] net::AddrParseError),
    #[error("Failed parsing PEM file {0}")]
    InvalidPemFile(PathBuf),
}
