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

    pub fn get_or_create(&self, db_name: &str) -> anyhow::Result<(Arc<DatabaseEngine>, Arc<AofLogger>)> {
        if let (Some(engine), Some(aof)) = (self.engines.get(db_name), self.aofs.get(db_name)) {
            return Ok((engine.clone(), aof.clone()));
        }

        // Create new
        let mut engine_raw = DatabaseEngine::new(db_name.to_string());
        engine_raw.max_connections = self.max_connections;
        let engine = Arc::new(engine_raw);
        let aof = Arc::new(AofLogger::new(db_name)?);

        self.engines.insert(db_name.to_string(), engine.clone());
        self.aofs.insert(db_name.to_string(), aof.clone());

        Ok((engine, aof))
    }

    pub fn get(&self, db_name: &str) -> Option<(Arc<DatabaseEngine>, Arc<AofLogger>)> {
        let engine = self.engines.get(db_name)?.clone();
        let aof = self.aofs.get(db_name)?.clone();
        Some((engine, aof))
    }
}
