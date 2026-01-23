use serde::{Deserialize, Serialize};
use crate::core::flexible::FlexibleStore;
use crate::core::structured::{StructuredStore, Table};
use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use serde_json::Value; // Add Value import
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    pub flexible: HashMap<String, Value>,
    pub structured: HashMap<String, Table>,
}

pub fn save_snapshot(flexible: &FlexibleStore, structured: &StructuredStore, db_name: &str) -> Result<()> {
    let snapshot = Snapshot {
        flexible: flexible.export(),
        structured: structured.export(),
    };

    let dir = std::env::var("DB_DATA_DIR").unwrap_or_else(|_| "data".to_string());
    std::fs::create_dir_all(&dir)?;
    let path = format!("{}/{}.snap.json", dir, db_name);
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer(writer, &snapshot)?;
    
    Ok(())
}

pub fn load_snapshot(db_name: &str) -> Result<Snapshot> {
    let dir = std::env::var("DB_DATA_DIR").unwrap_or_else(|_| "data".to_string());
    let path = format!("{}/{}.snap.json", dir, db_name);
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let snapshot: Snapshot = serde_json::from_reader(reader)?;
    Ok(snapshot)
}
