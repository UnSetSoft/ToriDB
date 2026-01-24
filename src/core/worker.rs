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
                            let (engine, aof, is_new) = match registry.get_or_create(&req.session.current_db) {
                                Ok(res) => res,
                                Err(e) => {
                                    let _ = req.resp_tx.send((req.session, format!("ERROR: Registry Failed: {}", e), None));
                                    continue;
                                }
                            };

                            // AOF Replay (Recovery)
                            if is_new {
                                if let Ok(cmds) = aof.load() {
                                    if !cmds.is_empty() {
                                        crate::core::logger::info(&format!("Replaying {} AOF commands for {}", cmds.len(), req.session.current_db));
                                        
                                        // Use a temporary session for replay
                                        let mut replay_session = Session {
                                            user: Some(crate::core::security::User {
                                                username: "system".to_string(),
                                                password: "".to_string(),
                                                rules: vec!["+@all".to_string()],
                                            }),
                                            _addr: "SYSTEM_RECOVERY".to_string(),
                                            connected_at: std::time::Instant::now(),
                                            current_db: req.session.current_db.clone(),
                                            tx_buffer: None,
                                        };

                                        for cmd_str in cmds {
                                             if let Ok((_, cmd)) = crate::net::parser::parse_command(&cmd_str) {
                                                 // Execute without re-logging
                                                 execute_command(&engine, cmd, &aof, &mut replay_session);
                                             }
                                        }
                                        crate::core::logger::info("AOF Replay complete.");
                                    }
                                }
                            }

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
                                crate::core::logger::info(&format!("Client {} writing data in {}", req.session._addr, req.session.current_db));
                                if let Err(e) = aof.log(&log_cmd) {
                                    crate::core::logger::error(&format!("AOF Error: {}", e));
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
