// Modules are now in lib.rs

use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use toridb::net::parser::parse_command;
use toridb::net::resp::{decode, RespValue};
use toridb::core::worker::WorkerPool;
use toridb::core::executor::Session;
use toridb::core::logger;
use toridb::core::registry::DatabaseRegistry;

use bytes::BytesMut;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut host = std::env::var("DB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let mut port = std::env::var("DB_PORT").unwrap_or_else(|_| "8569".to_string());
    let mut db_name = std::env::var("DB_NAME").unwrap_or_else(|_| "data".to_string());
    let mut data_dir = std::env::var("DB_DATA_DIR").unwrap_or_else(|_| "data".to_string());
    let mut workers = std::env::var("DB_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let mut max_connections = 100;
    
    // Check DB_URI
    if let Ok(uri_str) = std::env::var("DB_URI") {
        if let Ok(uri) = toridb::core::uri::ConnectionUri::parse(&uri_str) {
            host = uri.host.clone();
            port = uri.port.to_string();
            db_name = uri.db_name_default();
            
            // Apply query arguments
            workers = uri.get_query_param("workers", workers);
            max_connections = uri.get_query_param("max_connections", max_connections);
            if let Some(d) = uri.query.get("data_dir") {
                data_dir = d.clone();
                unsafe { std::env::set_var("DB_DATA_DIR", &data_dir); }
            }
        }
    }

    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr).await?; 
    logger::info(&format!("ToriDB Server running on {} (DB: {}, Data Dir: {})", addr, db_name, data_dir));

    // Initialize Registry and Worker Pool
    let registry = Arc::new(DatabaseRegistry::new(max_connections));
    let worker_pool = WorkerPool::new(workers, registry.clone());

    // We no longer need to manually load engine/aof here, 
    // it will be loaded by workers when first accessed.
    
    loop {
        let (mut socket, addr) = listener.accept().await?;
        let worker_pool = worker_pool.clone();
        let current_db = db_name.clone();
        let addr_str = addr.to_string();
        
        logger::info(&format!("New connection from {}", addr_str));

        tokio::spawn(async move {
            let mut buffer = BytesMut::with_capacity(4096);
            let mut session = Session { 
                user: None, 
                _addr: addr_str.clone(),
                connected_at: std::time::Instant::now(),
                current_db,
            };

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
                            // Execute via Worker Pool
                            let (new_session, res) = match worker_pool.execute(command, input_str.clone(), session).await {
                                Ok((s, r, _hash)) => (s, r),
                                Err(e) => {
                                    logger::error(&format!("Internal Worker Error: {}", e));
                                    let _ = socket.write_all(format!("-ERR Internal Worker Error: {}\r\n", e).as_bytes()).await;
                                    return; // Terminate connection as session is lost
                                }
                            };
                            session = new_session;
                            res
                        },
                        Err(_) => "ERROR: Syntax Error".to_string(),
                    };
                    
                    // Handle PSYNC - switch to replica propagation mode
                    if response == "_PSYNC_OK" {
                        // PSYNC currently needs careful handling with multi-db. 
                        // For now we assume they sync the 'current' DB or the default.
                        let (engine, _) = worker_pool.registry.get_or_create(&session.current_db).unwrap();

                        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1024);
                        engine.replication.add_replica(addr_str.clone(), tx);
                        
                        // Full Sync: Send current state as commands
                        let snapshot_cmds = engine.generate_rewrite_commands();
                        let _ = socket.write_all(format!("+FULLRESYNC {} {}\r\n", snapshot_cmds.len(), 0).as_bytes()).await;
                        for cmd in snapshot_cmds {
                            let resp_cmd = format!("${}\r\n{}\r\n", cmd.len(), cmd);
                            if socket.write_all(resp_cmd.as_bytes()).await.is_err() {
                                engine.replication.replicas.remove(&addr_str);
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
            logger::info(&format!("Client disconnected: {}", addr_str));
        });
    }
}
