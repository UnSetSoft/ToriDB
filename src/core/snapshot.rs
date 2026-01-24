use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter};
use std::sync::Arc;
use crate::core::memory::DatabaseEngine;
use crate::core::structured::Table;

#[derive(Serialize, Deserialize)]
pub struct SnapshotData {
    pub flexible_data: HashMap<String, Value>,
    pub structured_data: HashMap<String, Table>,
    pub timestamp: u64,
}

pub struct SnapshotManager;

impl SnapshotManager {
    pub fn save(engine: &Arc<DatabaseEngine>, path: &str) -> io::Result<()> {
        let flexible = engine.flexible.export();
        let structured = engine.structured.export();
        
        let snapshot = SnapshotData {
            flexible_data: flexible,
            structured_data: structured,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &snapshot)?;
        
        Ok(())
    }

    pub fn load(path: &str) -> io::Result<SnapshotData> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let snapshot: SnapshotData = serde_json::from_reader(reader)?;
        Ok(snapshot)
    }

    pub fn to_string(engine: &Arc<DatabaseEngine>) -> io::Result<String> {
        let flexible = engine.flexible.export();
        let structured = engine.structured.export();
        
        let snapshot = SnapshotData {
            flexible_data: flexible,
            structured_data: structured,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        serde_json::to_string(&snapshot).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub fn from_string(data: &str) -> io::Result<SnapshotData> {
        serde_json::from_str(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}
