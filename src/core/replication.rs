use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq)]
pub enum ValidRole {
    Master,
    Replica { master_addr: String, master_port: u16 },
}

use dashmap::DashMap;
use tokio::sync::mpsc;

pub struct ReplicationManager {
    pub role: Arc<RwLock<ValidRole>>,
    pub replicas: Arc<DashMap<String, mpsc::Sender<String>>>,
}

impl ReplicationManager {
    pub fn new() -> Self {
        Self {
            role: Arc::new(RwLock::new(ValidRole::Master)),
            replicas: Arc::new(DashMap::new()),
        }
    }

    pub fn add_replica(&self, addr: String, sender: mpsc::Sender<String>) {
        self.replicas.insert(addr, sender);
    }
    
    pub fn propagate(&self, command: &str) {
         // If we are master, broadcast
         if self.is_master() {
             for r in self.replicas.iter() {
                 let _ = r.value().try_send(command.to_string());
             }
         }
    }

    pub fn set_replica_of(&self, host: String, port: u16) {
        let mut w = self.role.write().unwrap();
        *w = ValidRole::Replica { master_addr: host.clone(), master_port: port };
        println!("Replication: Switched to Replica of {}:{}", host, port);
        // TODO: Spawn connection task
    }

    pub fn set_master(&self) {
        let mut w = self.role.write().unwrap();
        *w = ValidRole::Master;
        println!("Replication: Switched to Master");
    }

    pub fn is_master(&self) -> bool {
         matches!(*self.role.read().unwrap(), ValidRole::Master)
    }
    
    pub fn get_role_string(&self) -> String {
        match &*self.role.read().unwrap() {
            ValidRole::Master => "role:master".to_string(),
            ValidRole::Replica { master_addr, master_port } => format!("role:replica\nmaster_host:{}\nmaster_port:{}", master_addr, master_port),
        }
    }
}

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::core::memory::DatabaseEngine;
use crate::core::executor::{execute_command, Session};
use crate::net::parser::parse_command;
use crate::net::resp::decode;
use bytes::BytesMut;

use crate::core::persistence::AofLogger;

#[allow(dead_code)]
pub fn start_replication_task(engine: Arc<DatabaseEngine>, aof: Arc<AofLogger>, host: String, port: u16) {
    tokio::spawn(async move {
        println!("Replication: Connecting to {}:{}...", host, port);
        match TcpStream::connect(format!("{}:{}", host, port)).await {
            Ok(mut stream) => {
                println!("Replication: Connected to Master.");
                
                // Handshake
                // 1. PING
                if let Err(e) = stream.write_all(b"*1\r\n$4\r\nPING\r\n").await {
                    eprintln!("Rep: Failed to send PING: {}", e); return;
                }
                
                let mut buffer = BytesMut::with_capacity(4096);
                let mut session = Session {
                    user: engine.security.get_user("default"), 
                    _addr: format!("master-{}:{}", host, port),
                    connected_at: std::time::Instant::now(),
                };
                
                loop {
                     let _ = match stream.read_buf(&mut buffer).await {
                        Ok(n) if n == 0 => { println!("Rep: Master closed connection."); break; },
                        Ok(n) => n,
                        Err(e) => { eprintln!("Rep: Read Error: {}", e); break; },
                    };
                    
                    while let Ok(Some(resp_val)) = decode(&mut buffer) {
                        if let Some(cmd_str) = resp_val.to_command_string() {
                            if let Ok((_, cmd)) = parse_command(&cmd_str) {
                                // Replicas should write to AOF as well
                                execute_command(&engine, cmd, &aof, &mut session);
                            }
                        }
                    }
                }
            },
            Err(e) => eprintln!("Replication: Failed to connect: {}", e),
        }
    });
}
