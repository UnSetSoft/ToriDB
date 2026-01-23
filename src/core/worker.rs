use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use crate::core::executor::{execute_command, Session};
use crate::query::Command;
use crate::core::registry::DatabaseRegistry;

pub struct CommandRequest {
    pub cmd: Command,
    pub raw_cmd: String,
    pub session: Session,
    // Returns: (Modified Session, Response String, AOF Hash info)
    pub resp_tx: oneshot::Sender<(Session, String, Option<String>)>,
}

#[derive(Clone)]
pub struct WorkerPool {
    sender: mpsc::Sender<CommandRequest>,
    pub registry: Arc<DatabaseRegistry>,
}

impl WorkerPool {
    pub fn new(size: usize, registry: Arc<DatabaseRegistry>) -> Self {
        let (tx, rx) = mpsc::channel::<CommandRequest>(1024);
        let rx = Arc::new(Mutex::new(rx));

        for _ in 0..size {
            let registry = registry.clone();
            let rx = rx.clone();

            tokio::spawn(async move {
                loop {
                    let req_opt = {
                        let mut locked_rx = rx.lock().await;
                        locked_rx.recv().await
                    };

                    match req_opt {
                        Some(mut req) => {
                            // Resolve engine and AOF dynamically
                            let (engine, aof) = match registry.get_or_create(&req.session.current_db) {
                                Ok(res) => res,
                                Err(e) => {
                                    let _ = req.resp_tx.send((req.session, format!("ERROR: Registry Failed: {}", e), None));
                                    continue;
                                }
                            };

                            let cmd_for_log = req.cmd.clone();
                            let (res, hash) = execute_command(&engine, req.cmd, &aof, &mut req.session);
                            
                            // AOF Logging Logic
                            let log_cmd = match &cmd_for_log {
                                Command::AclSetUser { username, rules, .. } => { // password masked/handled via hash
                                    if let Some(h) = &hash {
                                        format!("ACL SETUSER {} \"{}\" {}", username, h, rules.join(" "))
                                    } else {
                                        req.raw_cmd.clone()
                                    }
                                }
                                _ => req.raw_cmd.clone(),
                            };

                            // Log if it is a write command
                            if cmd_for_log.is_write() {
                                if let Err(e) = aof.log(&log_cmd) {
                                    eprintln!("AOF Error: {}", e);
                                }
                                // Propagate to replicas
                                engine.replication.propagate(&log_cmd);
                            }

                            let _ = req.resp_tx.send((req.session, res, hash));
                        }
                        None => break,
                    }
                }
            });
        }

        Self { 
            sender: tx,
            registry,
        }
    }

    pub async fn execute(&self, cmd: Command, raw_cmd: String, session: Session) -> Result<(Session, String, Option<String>), String> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = CommandRequest {
            cmd,
            raw_cmd,
            session,
            resp_tx,
        };

        self.sender.send(req).await.map_err(|_| "Worker pool closed".to_string())?;
        
        resp_rx.await.map_err(|_| "Worker dropped request".to_string())
    }
}
