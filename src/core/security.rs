use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use crate::query::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password: String, // String for now, could be hashed later
    pub rules: Vec<String>, // Redis-like rules: "+@all", "-set", "+get"
}

impl User {
    pub fn can_execute(&self, cmd: &Command) -> bool {
        let cmd_name = match cmd {
            Command::ReplicaOf { .. } => "admin", // Requires admin/all permissions
            Command::Set { .. } => "set",
            Command::Get { .. } => "get",
            Command::Ttl { .. } => "ttl",
            Command::Incr { .. } => "incr",
            Command::Decr { .. } => "decr",
            Command::LPush { .. } => "lpush",
            Command::RPush { .. } => "rpush",
            Command::LPop { .. } => "lpop",
            Command::RPop { .. } => "rpop",
            Command::LRange { .. } => "lrange",
            Command::HSet { .. } => "hset",
            Command::HGet { .. } => "hget",
            Command::HGetAll { .. } => "hgetall",
            Command::SAdd { .. } => "sadd",
            Command::SMembers { .. } => "smembers",
            Command::JsonGet { .. } => "jsonget",
            Command::JsonSet { .. } => "jsonset",
            Command::CreateTable { .. } => "createtable",
            Command::AlterTable { .. } => "altertable",
            Command::Insert { .. } => "insert",
            Command::Select { .. } => "select",
            Command::Update { .. } => "update",
            Command::Delete { .. } => "delete",
            Command::Del { .. } => "delete",
            Command::CreateIndex { .. } => "createindex",
            Command::AclSetUser { .. } => "acl",
            Command::AclList => "acl",
            Command::AclGetUser { .. } => "acl",
            Command::AclDelUser { .. } => "acl",
            Command::Auth { .. } => "auth",
            Command::Ping => "ping",
            Command::Save => "save",
            Command::RewriteAof => "rewriteaof",
            Command::SetEx { .. } => "setex",
            Command::ClientList => "client",
            Command::ClientKill { .. } => "client",
            Command::Psync => "admin",
            Command::Info => "info",
            Command::ClusterInfo => "cluster",
            Command::ClusterSlots => "cluster",
            Command::ClusterMeet { .. } => "cluster",
            Command::ClusterAddSlots { .. } => "cluster",
            Command::ZAdd { .. } => "zadd",
            Command::ZRange { .. } => "zrange",
            Command::ZScore { .. } => "zscore",
            Command::Use { .. } => "use",
            Command::Begin => "transaction",
            Command::Commit => "transaction",
            Command::Rollback => "transaction",
            Command::VectorSearch { .. } => "select",
        };

        // Simplified rule checking
        if self.rules.contains(&"+@all".to_string()) {
            return true;
        }

        if self.rules.contains(&format!("-{}", cmd_name)) {
            return false;
        }

        if self.rules.contains(&format!("+{}", cmd_name)) {
            return true;
        }

        false
    }
}

pub struct SecurityStore {
    users: DashMap<String, User>,
}

impl SecurityStore {
    pub fn new() -> Self {
        let store = Self {
            users: DashMap::new(),
        };
        
        // Default admin user
        let default_pass = std::env::var("DB_PASSWORD").unwrap_or_else(|_| "secret".to_string());
        
        // Hash the default password
        let hashed = bcrypt::hash(default_pass, bcrypt::DEFAULT_COST).unwrap_or_else(|_| "bcrypt_failed".to_string());
        
        store.users.insert("default".to_string(), User {
            username: "default".to_string(),
            password: hashed,
            rules: vec!["+@all".to_string()],
        });
        
        store
    }

    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        if let Some(user) = self.users.get(username) {
            // Verify hash
            return bcrypt::verify(password, &user.password).unwrap_or(false);
        }
        false
    }

    pub fn get_user(&self, username: &str) -> Option<User> {
        self.users.get(username).map(|u| u.clone())
    }

    pub fn set_user(&self, mut user: User) -> String {
        // If password is already a bcrypt hash, don't re-hash (useful for AOF replay)
        if user.password.starts_with("$2a$") || user.password.starts_with("$2b$") || user.password.starts_with("$2y$") {
            self.users.insert(user.username.clone(), user.clone());
            return user.password;
        }

        // Hash password before saving
        if let Ok(hashed) = bcrypt::hash(&user.password, bcrypt::DEFAULT_COST) {
            user.password = hashed.clone();
            self.users.insert(user.username.clone(), user);
            return hashed;
        }
        "error".to_string()
    }

    pub fn delete_user(&self, username: &str) {
        self.users.remove(username);
    }

    pub fn list_users(&self) -> Vec<String> {
        self.users.iter().map(|kv| kv.key().clone()).collect()
    }
}
