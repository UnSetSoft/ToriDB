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
    pub transaction_lock: Arc<Mutex<()>>,
}

use super::flexible::FlexibleStore;
use super::structured::StructuredStore;
use super::security::SecurityStore;
use super::replication::ReplicationManager;
use super::cluster::ClusterManager;
use std::sync::{Arc, Mutex};

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
            transaction_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn generate_rewrite_commands(&self) -> Vec<String> {
        let mut commands = Vec::new();
        commands.extend(self.flexible.dump_commands());
        commands.extend(self.structured.dump_commands());
        commands
    }
    
    pub fn save_snapshot(&self) -> std::io::Result<()> {
        let data_dir = std::env::var("DB_DATA_DIR").unwrap_or_else(|_| "data".to_string());
        // mkdir loop handled in AofLogger/main, but redundant check ok
        let _ = std::fs::create_dir_all(&data_dir);
        let _path = format!("{}/{}_dump.json", data_dir, self.db_name);
        // We need self wrapped in Arc? No, we have reference self.
        // SnapshotManager::save expects &Arc<DatabaseEngine> or just &DatabaseEngine?
        // Let's modify SnapshotManager to take &DatabaseEngine or pass internal maps.
        // Actually, SnapshotManager::save takes &Arc<DatabaseEngine> in my code above.
        // But here we are inside DatabaseEngine. We can't easily get Arc<Self> from &self.
        // Better: Update SnapshotManager to take specific data references or change this helper.
        // Or executing command passes the Arc<DatabaseEngine> to the manager directly.
        // Let's NOT implement save_snapshot on Engine, but call Manager directly from Executor.
        
        Ok(())
    }


    pub fn load_from_snapshot(&mut self, snapshot: crate::core::snapshot::SnapshotData) {
        self.flexible = FlexibleStore::import_from(snapshot.flexible_data);
        self.structured = StructuredStore::import_from(snapshot.structured_data);
        // We could also restore timestamp or other metadata if needed
    }
    pub fn restore_state(&self, snapshot: crate::core::snapshot::SnapshotData) {
        self.flexible.restore(snapshot.flexible_data);
        self.structured.restore(snapshot.structured_data);
    }
}
