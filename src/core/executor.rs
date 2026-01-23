use crate::core::memory::DatabaseEngine;
use crate::query::Command;
use crate::core::structured::{Column, DataType};
use crate::core::persistence::AofLogger;
use crate::core::security::User;
use crate::core::logger;

pub struct Session {
    pub user: Option<User>,
    pub _addr: String,
    pub connected_at: std::time::Instant,
    pub current_db: String,
}

use std::sync::Arc;

pub fn execute_command(engine: &Arc<DatabaseEngine>, cmd: Command, aof: &AofLogger, session: &mut Session) -> (String, Option<String>) {
    // 1. Handle AUTH (always allowed to attempt)
    if let Command::Auth { ref username, ref password } = cmd {
        let target_user = username.as_deref().unwrap_or("default");
        if engine.security.authenticate(target_user, password) {
            session.user = engine.security.get_user(target_user);
            logger::info(&format!("Client {} authenticated as user '{}'", session._addr, target_user));
            return ("OK".to_string(), None);
        } else {
            logger::warn(&format!("Authentication failed for client {} as user '{}'", session._addr, target_user));
            return ("ERROR: Invalid password".to_string(), None);
        }
    }

    // 2. Check if authenticated
    let user = match &session.user {
        Some(u) => u,
        None => return ("ERROR: Authentication required".to_string(), None),
    };

    // 3. Check permissions
    if !user.can_execute(&cmd) {
        logger::warn(&format!("Permission denied: client {} (user '{}') attempted unauthorized command: {:?}", session._addr, user.username, cmd));
        return (format!("ERROR: User '{}' has no permissions for this command", user.username), None);
    }
    
    // 4. Check Sharding Slot Ownership
    if let Some(key) = cmd.get_key() {
        if !engine.cluster.owns_slot(key) {
            let slot = crate::core::cluster::ClusterManager::key_slot(key);
            if let Some(addr) = engine.cluster.get_redirect(key) {
                return (format!("MOVED {} {}", slot, addr), None);
            } else {
                // If we don't know who owns it, return internal error or assume we should have it?
                // In a proper cluster, we'd know. For now, just allow if unknown or return error.
            }
        }
    }

    // 5. Check Replica Read-Only Mode
    if !engine.replication.is_master() {
        if cmd.is_write() {
            if let Command::ReplicaOf { .. } = cmd {
                // Allowed
            } else {
                 return ("ERROR: READONLY You can't write against a read only replica.".to_string(), None);
            }
        }
    }

    match cmd {
        Command::ReplicaOf { host, port } => {
            if host.to_uppercase() == "NO" && port.to_uppercase() == "ONE" {
                engine.replication.set_master();
                ("OK".to_string(), None)
            } else if host.starts_with("db://") {
                match crate::core::uri::ConnectionUri::parse(&host) {
                    Ok(uri) => {
                         engine.replication.set_replica_of(uri.host, uri.port);
                        ("OK".to_string(), Some("_CONNECT_TO_MASTER".to_string()))
                    }
                    Err(e) => (format!("ERROR: Invalid URI: {}", e), None)
                }
            } else {
                if let Ok(p) = port.parse::<u16>() {
                    engine.replication.set_replica_of(host.clone(), p);
                    // Note: Actual connection is started by the caller (main.rs) if needed, 
                    // or via start_replication_task. For now, just set role.
                    // We can't spawn here due to AofLogger ownership issues. 
                    // Return special marker so caller spawns connection.
                     ("OK".to_string(), Some("_CONNECT_TO_MASTER".to_string()))
                } else {
                     ("ERROR: Invalid port".to_string(), None)
                }
            }
        }
        Command::Psync => {
            // This is called by replicas connecting to us (master).
            // We return a marker so the caller (main.rs) can register this as a replica.
            ("_PSYNC_OK".to_string(), None)
        }
        Command::Ping => ("PONG".to_string(), None),
        Command::Save => {
            match crate::core::snapshot::save_snapshot(&engine.flexible, &engine.structured, &engine.db_name) {
                Ok(_) => ("OK".to_string(), None),
                Err(e) => {
                    logger::error(&format!("Snapshot Save failed: {}", e));
                    (format!("ERROR: {}", e), None)
                },
            }
        }
        Command::RewriteAof => {
            let cmds = engine.generate_rewrite_commands();
            match aof.rewrite(cmds) {
                 Ok(_) => ("OK".to_string(), None),
                 Err(e) => {
                    logger::error(&format!("AOF Rewrite failed: {}", e));
                    (format!("ERROR: AOF Rewrite failed: {}", e), None)
                 },
            }
        }
        Command::Info => {
            let role = engine.replication.get_role_string();
            let clients = engine.clients.len();
            let max_clients = engine.max_connections;
            let info = format!(
                "# Server\r\nversion:0.1.0\r\n\r\n# Clients\r\nconnected_clients:{}\r\nmax_clients:{}\r\n\r\n# Replication\r\n{}\r\nconnected_replicas:{}\r\n",
                clients, max_clients, role, engine.replication.replicas.len()
            );
            (info, None)
        }
        Command::ClusterInfo => {
            (engine.cluster.get_info(), None)
        }
        Command::ClusterSlots => {
            // Return slot ranges (simplified)
            let mut result = String::new();
            for entry in engine.cluster.nodes.iter() {
                for range in entry.value() {
                    result.push_str(&format!("{}-{} {}\n", range.start, range.end, entry.key()));
                }
            }
            if result.is_empty() {
                result = "0-16383 127.0.0.1:8569 (standalone)\n".to_string();
            }
            (result, None)
        }
        Command::ClusterMeet { host, port } => {
            let addr = format!("{}:{}", host, port);
            engine.cluster.add_node(addr);
            ("OK".to_string(), None)
        }
        Command::ClusterAddSlots { slots } => {
            engine.cluster.add_slots(slots);
            ("OK".to_string(), None)
        }
        Command::Use { db_name } => {
            if session.current_db != db_name {
                logger::info(&format!("Client {} switched to database: {}", session._addr, db_name));
                session.current_db = db_name;
            }
            ("OK".to_string(), None)
        }
        // ACL Commands
        Command::AclSetUser { username, password, rules } => {
            let hash = engine.security.set_user(User { username, password, rules });
            ("OK".to_string(), Some(hash))
        }
        Command::AclGetUser { username } => {
            (match engine.security.get_user(&username) {
                Some(u) => format!("username: {}\nrules: {:?}", u.username, u.rules),
                None => "ERROR: User not found".to_string(),
            }, None)
        }
        Command::AclList => {
            (format!("{:?}", engine.security.list_users()), None)
        }
        Command::AclDelUser { username } => {
            engine.security.delete_user(&username);
            ("OK".to_string(), None)
        }
        Command::Set { key, value } => {
            // Try parsing JSON, else store as string
            let json_val = serde_json::from_str(&value).unwrap_or(serde_json::Value::String(value));
            engine.flexible.set(key, json_val);
            ("OK".to_string(), None)
        }
        Command::Get { key } => {
            (match engine.flexible.get(&key) {
                Some(val) => {
                    if let Some(s) = val.as_str() { s.to_string() } else { format!("{}", val) }
                }
                None => "nil".to_string(),
            }, None)
        }
        // Lists
        Command::LPush { key, values } => {
            let len = engine.flexible.lpush(&key, values);
            (format!("(integer) {}", len), None)
        }
        Command::RPush { key, values } => {
            let len = engine.flexible.rpush(&key, values);
            (format!("(integer) {}", len), None)
        }
        Command::LPop { key, count } => {
            let res = engine.flexible.lpop(&key, count.unwrap_or(1));
            (format!("{:?}", res), None)
        }
        Command::RPop { key, count } => {
            let res = engine.flexible.rpop(&key, count.unwrap_or(1));
            (format!("{:?}", res), None)
        }
        Command::LRange { key, start, stop } => {
            let res = engine.flexible.lrange(&key, start, stop);
             (format!("{:?}", res), None)
        }
        // Hashes
        Command::HSet { key, field, value } => {
            let new = engine.flexible.hset(&key, field, value);
            (format!("(integer) {}", new), None)
        }
        Command::HGet { key, field } => {
            (match engine.flexible.hget(&key, &field) {
                Some(val) => val,
                None => "nil".to_string(),
            }, None)
        }
        Command::HGetAll { key } => {
            let res = engine.flexible.hgetall(&key);
            (format!("{:?}", res), None)
        }
        // Sets
        // Client/Management
        Command::ClientList => {
            let mut list = String::new();
            for kv in engine.clients.iter() {
                let info = kv.value();
                list.push_str(&format!("addr={} user={} age={}s\n", 
                    info.addr, info.user, info.connected_at.elapsed().as_secs()));
            }
            (list, None)
        }
        Command::ClientKill { addr } => {
            // This is "soft kill" since we don't have a global socket registry
            // But we can remove from registry. Actual disconnect happens on next IO.
            // A more robust implementation would use a mpsc channel per socket.
            engine.clients.remove(&addr);
            ("OK".to_string(), None)
        }
        Command::SAdd { key, members } => {
            let added = engine.flexible.sadd(&key, members);
            (format!("(integer) {}", added), None)
        }
        Command::SMembers { key } => {
            let res = engine.flexible.smembers(&key);
            (format!("{:?}", res), None)
        }
        // ZSET (Sorted Sets)
        Command::ZAdd { key, score, member } => {
            let added = engine.flexible.zadd(&key, score, member);
            (format!("(integer) {}", added), None)
        }
        Command::ZRange { key, start, stop } => {
            let res = engine.flexible.zrange(&key, start, stop);
            (format!("{:?}", res), None)
        }
        Command::ZScore { key, member } => {
            match engine.flexible.zscore(&key, &member) {
                Some(score) => (format!("{}", score), None),
                None => ("nil".to_string(), None),
            }
        }
        // JSON
        Command::JsonGet { key, path } => {
            (match engine.flexible.json_get(&key, path.as_deref()) {
                Some(val) => format!("{}", val),
                None => "nil".to_string(),
            }, None)
        }
        Command::JsonSet { key, path, value } => {
            // Parse value as JSON first
            if let Ok(json_val) = serde_json::from_str(&value) {
                let res = engine.flexible.json_set(&key, &path, json_val);
                (format!("(integer) {}", res), None)
            } else {
                 (format!("ERROR: Invalid JSON value"), None)
            }
        }
        Command::CreateTable { name, columns } => {
            let cols: Vec<Column> = columns.iter().map(|(n, t, pk, fk)| {
                let dt = match t.to_uppercase().as_str() {
                    "INT" | "INTEGER" => DataType::Integer,
                    "BOOL" | "BOOLEAN" => DataType::Boolean,
                    "FLOAT" | "DOUBLE" => DataType::Float,
                    "DATETIME" | "TIMESTAMP" => DataType::DateTime,
                    "BLOB" | "BYTES" => DataType::Blob,
                    _ => DataType::String,
                };
                Column {
                    name: n.clone(), 
                    data_type: dt,
                    is_primary_key: *pk,
                    references: fk.clone(),
                }
            }).collect();
            
            match engine.structured.create_table(name, cols) {
                Ok(_) => ("OK".to_string(), None),
                Err(e) => (format!("ERROR: {}", e), None),
            }
        }
        Command::AlterTable { table, op } => {
            match engine.structured.alter_table(&table, op) {
                Ok(_) => ("OK".to_string(), None),
                Err(e) => (format!("ERROR: {}", e), None),
            }
        }
        Command::Insert { table, values } => {
            match engine.structured.insert(&table, values) {
                Ok(_) => ("OK".to_string(), None),
                Err(e) => (format!("ERROR: {}", e), None),
            }
        }
        Command::Select { table, selector, filter, group_by, having, order_by, limit, offset } => {
            match engine.structured.select(&table, selector, filter, group_by, having, order_by, limit, offset) {
                Ok(rows) => {
                    let mut res = String::new();
                    for row in rows {
                        res.push_str(&format!("{:?}\n", row));
                    }
                    (if res.is_empty() { "EMPTY".to_string() } else { res.trim_end().to_string() }, None)
                },
                Err(e) => (format!("ERROR: {}", e), None),
            }
        }
        Command::Update { table, filter, set } => {
            match engine.structured.update(&table, filter, set) {
                Ok(_) => ("OK".to_string(), None),
                Err(e) => (format!("ERROR: {}", e), None),
            }
        }
        Command::Delete { table, filter } => {
            match engine.structured.delete(&table, filter) {
                Ok(_) => ("OK".to_string(), None),
                Err(e) => (format!("ERROR: {}", e), None),
            }
        }
        Command::CreateIndex { index_name, table, column } => {
            match engine.structured.create_index(&index_name, &table, &column) {
                Ok(_) => ("OK".to_string(), None),
                Err(e) => (format!("ERROR: {}", e), None),
            }
        }
        Command::SetEx { key, value, ttl } => {
            let json_val = serde_json::from_str(&value).unwrap_or(serde_json::Value::String(value));
            engine.flexible.set_with_ttl(key, json_val, ttl);
            ("OK".to_string(), None)
        }
        Command::Ttl { key } => {
            (match engine.flexible.ttl(&key) {
                Some(ttl) => format!("{}", ttl),
                None => "-2".to_string(),
            }, None)
        }
        Command::Auth { .. } => ("OK".to_string(), None), // Handled at start
        Command::Incr { key } => {
            let val = engine.flexible.incr(&key);
            (format!("{}", val), None)
        }
        Command::Decr { key } => {
            let val = engine.flexible.decr(&key);
            (format!("{}", val), None)
        }
    }
}
