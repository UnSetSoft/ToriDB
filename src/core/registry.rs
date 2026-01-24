use dashmap::DashMap;
use std::sync::Arc;
use crate::core::memory::DatabaseEngine;
use crate::core::persistence::AofLogger;

pub struct DatabaseRegistry {
    engines: DashMap<String, Arc<DatabaseEngine>>,
    aofs: DashMap<String, Arc<AofLogger>>,
    pub max_connections: usize,
}

impl DatabaseRegistry {
    pub fn new(max_connections: usize) -> Self {
        Self {
            engines: DashMap::new(),
            aofs: DashMap::new(),
            max_connections,
        }
    }

    pub fn get_or_create(&self, db_name: &str) -> anyhow::Result<(Arc<DatabaseEngine>, Arc<AofLogger>, bool)> {
        if let (Some(engine), Some(aof)) = (self.engines.get(db_name), self.aofs.get(db_name)) {
            return Ok((engine.clone(), aof.clone(), false));
        }

        // Create new
        let mut engine_raw = DatabaseEngine::new(db_name.to_string());
        engine_raw.max_connections = self.max_connections;

        // Recovery: Check for Snapshot if AOF doesn't exist (assuming AOF is preferred source of truth)
        let data_dir = std::env::var("DB_DATA_DIR").unwrap_or_else(|_| "data".to_string());
        let aof_path = format!("{}/{}.db", data_dir, db_name);
        
        // Only load snapshot if AOF does not exist (start fresh or restore) 
        // OR if we implement AOF-on-top-of-Snapshot logic later.
        if !std::path::Path::new(&aof_path).exists() {
             let dump_path = format!("{}/{}_dump.json", data_dir, db_name);
             if std::path::Path::new(&dump_path).exists() {
                 crate::core::logger::info(&format!("Loading Snapshot for {}...", db_name));
                 match crate::core::snapshot::SnapshotManager::load(&dump_path) {
                     Ok(snap) => {
                         engine_raw.load_from_snapshot(snap);
                         crate::core::logger::info("Snapshot loaded successfully.");
                     },
                     Err(e) => {
                         crate::core::logger::error(&format!("Failed to load snapshot: {}", e));
                     }
                 }
             }
        }

        let engine = Arc::new(engine_raw);
        let aof = Arc::new(AofLogger::new(db_name)?);

        crate::core::logger::info(&format!("Creating new database: {}", db_name));

        self.engines.insert(db_name.to_string(), engine.clone());
        self.aofs.insert(db_name.to_string(), aof.clone());

        Ok((engine, aof, true))
    }

    pub fn get(&self, db_name: &str) -> Option<(Arc<DatabaseEngine>, Arc<AofLogger>)> {
        let engine = self.engines.get(db_name)?.clone();
        let aof = self.aofs.get(db_name)?.clone();
        Some((engine, aof))
    }
}
