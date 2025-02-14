/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::sync::Arc;

use log::warn;
use tokio::sync::{mpsc, watch};

use rpc::MsgStream;

use crate::daemon::DbDaemon;
use dbdaemon_api::{BackendDbService, Request, Response};

pub async fn handle_conn<S: MsgStream<Response, Request>>(
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
                    Ok(req) => {
                        match tx.clone().reserve_owned().await {
                            Ok(permit) => {
                                tokio::spawn(handle_request(req, permit, service.clone()));
                            },
                            Err(_) => {
                                let res = Response {
                                    req_id: req.req_id,
                                    response: Err("shutdown in progress".to_string())
                                };
                                if let Err(e) = stream.send(res).await {
                                    println!("Error sending response over socket: {:?}", e);
                                }
                            }
                        }
                    },
                    Err(err) => match err {
                        rpc::Error::Json(err) => {
                            warn!("Failed to decode request (invalid JSON): {}", err);
                        },
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
            },
            r = term.changed() => match r {
                Ok(()) => if *term.borrow() { rx.close(); }
                Err(e) => {
                    warn!("Term signal channel failed: {}", e);
                    rx.close();
                }
            }
        };
    }
}

async fn handle_request(
    req: Request,
    tx: mpsc::OwnedPermit<Response>,
    service: Arc<DbDaemon>,
) {
    tx.send(Response {
        req_id: req.req_id,
        response: match serde_json::from_value(req.request) {
            Ok(req) => service.request(req).await.map_err(|e| e.to_string()),
            Err(e) => Err(format!("failed to deserialize request: {}", e)),
        },
    });
}
