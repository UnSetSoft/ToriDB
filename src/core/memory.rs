use dashmap::DashMap;

#[derive(Clone)]
pub struct ClientInfo {
    pub addr: String,
    pub user: String,
    pub connected_at: std::time::Instant,
}

#[derive(Clone)]
pub struct DatabaseEngine {
    pub db_name: String,
    pub flexible: FlexibleStore,
    pub structured: StructuredStore,
    pub security: Arc<SecurityStore>,
    pub clients: Arc<DashMap<String, ClientInfo>>,
    pub replication: Arc<ReplicationManager>,
    pub cluster: Arc<ClusterManager>,
    pub max_connections: usize,
}

use super::flexible::FlexibleStore;
use super::structured::StructuredStore;
use super::security::SecurityStore;
use super::replication::ReplicationManager;
use super::cluster::ClusterManager;
use std::sync::Arc;

impl DatabaseEngine {
    pub fn new(db_name: String) -> Self {
        Self {
            db_name,
            flexible: FlexibleStore::new(),
            structured: StructuredStore::new(),
            security: Arc::new(SecurityStore::new()),
            clients: Arc::new(DashMap::new()),
            replication: Arc::new(ReplicationManager::new()),
            cluster: Arc::new(ClusterManager::new()),
            max_connections: 100, // Default limit
        }
    }

    pub fn generate_rewrite_commands(&self) -> Vec<String> {
        let mut commands = Vec::new();
        commands.extend(self.flexible.dump_commands());
        commands.extend(self.structured.dump_commands());
        commands
    }
}
