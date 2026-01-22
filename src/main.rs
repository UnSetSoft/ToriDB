mod core;
mod net;
mod query;

use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use crate::core::memory::DatabaseEngine;
use crate::net::parser::parse_command;
use crate::net::resp::{decode, RespValue};
use crate::core::persistence::AofLogger;
use crate::core::worker::WorkerPool;
use crate::core::executor::{Session, execute_command};

use bytes::BytesMut;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut host = std::env::var("DB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let mut port = std::env::var("DB_PORT").unwrap_or_else(|_| "8569".to_string());
    let mut db_name = std::env::var("DB_NAME").unwrap_or_else(|_| "data".to_string());
    let data_dir = std::env::var("DB_DATA_DIR").unwrap_or_else(|_| "data".to_string());
    
    // Check DB_URI
    if let Ok(uri_str) = std::env::var("DB_URI") {
        if let Ok(uri) = crate::core::uri::ConnectionUri::parse(&uri_str) {
            host = uri.host.clone();
            port = uri.port.to_string();
            db_name = uri.db_name_default();
        }
    }

    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr).await?; 
    println!("Database Server running on {} (DB: {})", addr, db_name);

    // Initialize Engine and AOF
    let mut engine = Arc::new(DatabaseEngine::new(db_name.clone()));
    let aof = Arc::new(AofLogger::new(&db_name)?);

    // Try loading snapshot first
    println!("Loading Snapshot for {}...", db_name);
    if let Ok(snapshot) = crate::core::snapshot::load_snapshot(&db_name) {
        engine = Arc::new(DatabaseEngine {
            db_name: db_name.clone(),
            flexible: crate::core::flexible::FlexibleStore::import_from(snapshot.flexible),
            structured: crate::core::structured::StructuredStore::import_from(snapshot.structured),
            security: engine.security.clone(),
            clients: engine.clients.clone(),
            replication: engine.replication.clone(),
            cluster: engine.cluster.clone(),
            max_connections: engine.max_connections,
        });
        println!("Snapshot Loaded.");
    } else {
        println!("No snapshot found or load failed (starting fresh/AOF).");
    }

    // Replay AOF
    println!("Loading AOF...");
    let mut system_session = Session { 
        user: engine.security.get_user("default"),
        _addr: "system".to_string(),
        connected_at: std::time::Instant::now(),
    };
    if let Ok(commands) = aof.load() {
         for cmd_str in commands {
             if let Ok((_, cmd)) = parse_command(&cmd_str) {
                 execute_command(&engine, cmd, &aof, &mut system_session);
             }
         }
         println!("AOF Loaded.");
    }

    // Initialize Worker Pool
    let workers = std::env::var("DB_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let worker_pool = WorkerPool::new(workers, engine.clone(), aof.clone());

    loop {
        let (mut socket, addr) = listener.accept().await?;
        let engine = engine.clone();
        // let aof = aof.clone(); // Not needed in main loop anymore, worker handles it
        let worker_pool = worker_pool.clone();
        
        let addr_str = addr.to_string();
        
        // Enforce max connections
        if engine.clients.len() >= engine.max_connections {
            let _ = socket.write_all(b"-ERR max number of clients reached\r\n").await;
            continue;
        }

        tokio::spawn(async move {
            let mut buffer = BytesMut::with_capacity(4096);
            let mut session = Session { 
                user: None, 
                _addr: addr_str.clone(),
                connected_at: std::time::Instant::now() 
            };
            
            // Register client
            engine.clients.insert(addr_str.clone(), crate::core::memory::ClientInfo {
                addr: addr_str.clone(),
                user: "unauthenticated".to_string(),
                connected_at: session.connected_at,
            });

            loop {
                let _n = match socket.read_buf(&mut buffer).await {
                    Ok(n) if n == 0 => break, 
                    Ok(n) => n,
                    Err(_) => break,
                };
                
                while let Ok(Some(resp_val)) = decode(&mut buffer) {
                    let input_str = match resp_val.to_command_string() {
                        Some(s) => s,
                        None => {
                            let _ = socket.write_all(b"-ERR invalid command format\r\n").await;
                            continue;
                        }
                    };

                    let response = match parse_command(&input_str) {
                        Ok((_, command)) => {
                            let old_user_name = session.user.as_ref().map(|u| u.username.clone()).unwrap_or("unauthenticated".into());
                            
                            // Execute via Worker Pool
                            let (new_session, res) = match worker_pool.execute(command, input_str.clone(), session).await {
                                Ok((s, r, _hash)) => (s, r),
                                Err(e) => {
                                    let _ = socket.write_all(format!("-ERR Internal Worker Error: {}\r\n", e).as_bytes()).await;
                                    return; // Terminate connection as session is lost
                                }
                            };
                            session = new_session;
                            
                            // Update client registry if user changed
                            let new_user_name = session.user.as_ref().map(|u| u.username.clone()).unwrap_or("unauthenticated".into());
                            if old_user_name != new_user_name {
                                engine.clients.insert(addr_str.clone(), crate::core::memory::ClientInfo {
                                    addr: addr_str.clone(),
                                    user: new_user_name,
                                    connected_at: session.connected_at,
                                });
                            }
                            res
                        },
                        Err(_) => "ERROR: Syntax Error".to_string(),
                    };
                    
                    // Handle PSYNC - switch to replica propagation mode
                    if response == "_PSYNC_OK" {
                        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1024);
                        engine.replication.add_replica(addr_str.clone(), tx);
                        
                        // Full Sync: Send current state as commands
                        let snapshot_cmds = engine.generate_rewrite_commands();
                        let _ = socket.write_all(format!("+FULLRESYNC {} {}\r\n", snapshot_cmds.len(), 0).as_bytes()).await;
                        for cmd in snapshot_cmds {
                            let resp_cmd = format!("${}\r\n{}\r\n", cmd.len(), cmd);
                            if socket.write_all(resp_cmd.as_bytes()).await.is_err() {
                                engine.replication.replicas.remove(&addr_str);
                                engine.clients.remove(&addr_str);
                                return;
                            }
                        }
                        let _ = socket.write_all(b"+SYNC_COMPLETE\r\n").await;
                        
                        // Propagation loop: forward commands to this replica
                        loop {
                            match rx.recv().await {
                                Some(cmd) => {
                                    // Send as RESP inline command (simplified)
                                    let resp_cmd = format!("${}\r\n{}\r\n", cmd.len(), cmd);
                                    if socket.write_all(resp_cmd.as_bytes()).await.is_err() {
                                        break;
                                    }
                                }
                                None => break, // Channel closed
                            }
                        }
                        // Replica loop ended, cleanup
                        engine.replication.replicas.remove(&addr_str);
                        engine.clients.remove(&addr_str);
                        return;
                    }
                    
                    let resp_out = if response == "nil" {
                        RespValue::BulkString(None)
                    } else if response.starts_with("ERROR:") {
                        RespValue::Error(response.replace("ERROR: ", "").trim().to_string())
                    } else if response.starts_with("(integer)") {
                        let val = response.replace("(integer) ", "").trim().parse::<i64>().unwrap_or(0);
                        RespValue::Integer(val)
                    } else if response == "OK" || response == "PONG" {
                        RespValue::SimpleString(response)
                    } else {
                        RespValue::BulkString(Some(response.as_bytes().to_vec()))
                    };

                    if let Err(_) = socket.write_all(&resp_out.serialize()).await {
                        break;
                    }
                }
            }
            // Unregister client
            engine.clients.remove(&addr_str);
        });
    }
}
