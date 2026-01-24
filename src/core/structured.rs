//! # Structured Store (Relational)
//! 
//! This module implements the relational engine of ToriDB.
//! It supports typed tables, secondary indexing, and ACID-compliant joins.
//! 
//! ## Storage Model
//! Tables are stored as a mapping of stable `u64` Row IDs to a vector of `UnifiedValue`.
//! This allows for efficient primary key lookups while maintaining insertion order.
//! 
//! ## Indexing
//! - **Standard Indexes**: Uses `DashMap` for equality lookups (O(1)).
//! - **Range Indexes**: Uses `BTreeMap` protected by `RwLock` for range queries and sorting (O(log n)).

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Included, Excluded, Unbounded};
use anyhow::{Result, anyhow};
use crate::query::{Operator, Filter, Selector, AlterOp, JoinClause};
use crate::core::types::UnifiedValue;

/// Supported Data Types for SQL Columns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    String,
    Boolean,
    Float,
    DateTime, // Stored as ISO8601 string
    Blob,     // Stored as Base64 string
    Json,     // Stored as UnifiedValue::Object or Array
    Vector,   // Stored as UnifiedValue::Vector
}

/// Represents a single column definition in a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub references: Option<(String, String)>, // (table, column)
}

/// In-memory representation of an SQL Table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub next_row_id: u64,
    /// Stable ID -> Column Values
    pub rows: BTreeMap<u64, Vec<UnifiedValue>>, 
}

/// The core registry for relational data and indexing.
#[derive(Clone)]
pub struct StructuredStore {
    /// Registry of tables: name -> table_instance (thread-safe)
    tables: Arc<DashMap<String, RwLock<Table>>>,
    /// Equality indexes: table_name -> col_name -> value -> row_ids
    indexes: Arc<DashMap<String, DashMap<String, DashMap<UnifiedValue, Vec<u64>>>>>,
    /// Sorted/Range indexes: table_name -> col_name -> BTreeMap<value, row_ids>
    range_indexes: Arc<DashMap<String, DashMap<String, RwLock<BTreeMap<UnifiedValue, Vec<u64>>>>>>,
}

impl StructuredStore {
    // For Snapshotting
    pub fn export(&self) -> std::collections::HashMap<String, Table> {
        let mut map = std::collections::HashMap::new();
        for kv in self.tables.iter() {
            let table_name = kv.key().clone();
            // We need to acquire read lock to clone the table
            if let Ok(table) = kv.value().read() {
                map.insert(table_name, table.clone());
            }
        }
        map
    }



    // For AOF Rewrite
    pub fn dump_commands(&self) -> Vec<String> {
        let mut commands = Vec::new();
        
        // 1. Tables and Data
        for kv in self.tables.iter() {
            if let Ok(table) = kv.value().read() {
                // CREATE TABLE
                let cols_def = table.columns.iter()
                    .map(|c| {
                        let type_str = match c.data_type {
                            DataType::Integer => "int",
                            DataType::String => "string",
                            DataType::Boolean => "bool",
                            DataType::Float => "float",
                            DataType::DateTime => "datetime",
                            DataType::Blob => "blob",
                            DataType::Json => "json",
                            DataType::Vector => "vector",
                        };
                        let base = if c.is_primary_key {
                            format!("{}:{}:pk", c.name, type_str)
                        } else {
                            format!("{}:{}", c.name, type_str)
                        };
                        
                        if let Some((ref t, ref col)) = c.references {
                            format!("{}:fk({}.{})", base, t, col)
                        } else {
                            base
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                
                commands.push(format!("CREATE TABLE {} {}", table.name, cols_def));

                // INSERTs
                for (_, row) in &table.rows {
                    let vals = row.iter()
                        .map(|v| match v {
                            UnifiedValue::String(s) => format!("\"{}\"", s), // Quote strings
                            UnifiedValue::DateTime(i) => format!("{}", i),
                            UnifiedValue::Blob(b) => format!("\"{}\"", b),
                            UnifiedValue::Object(_) | UnifiedValue::Array(_) => {
                                // Serialize JSON back to string
                                serde_json::to_string(v).unwrap_or_else(|_| "{}".to_string())
                            },
                            _ => format!("{}", v), // Display impl handles others
                        }) 
                        .collect::<Vec<_>>()
                        .join(" ");
                    commands.push(format!("INSERT {} {}", table.name, vals));
                }
            }
        }

        // 2. Indexes
        for kv in self.indexes.iter() {
            let table_name = kv.key();
            for col_entry in kv.value().iter() {
                let col_name = col_entry.key();
                // CREATE INDEX idx_table_col ON table(col)
                commands.push(format!("CREATE INDEX idx_{}_{} ON {}({})", table_name, col_name, table_name, col_name));
            }
        }

        commands
    }

    pub fn import_from(tables: std::collections::HashMap<String, Table>) -> Self {
        let store = Self::new();
        for (name, table) in tables {
            // Rebuild PK/Unique indexes
            // We need to identify which columns need indexing.
            // Currently only PK is auto-indexed.
            let idx_cols: Vec<String> = table.columns.iter()
                .filter(|c| c.is_primary_key)
                .map(|c| c.name.clone())
                .collect();

            // Insert table
            store.tables.insert(name.clone(), std::sync::RwLock::new(table));

            // Create indices
            for col in idx_cols {
                let _ = store.create_index(&name, &col, "HASH");
            }
        }
        store
    }
}

impl StructuredStore {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(DashMap::new()),
            indexes: Arc::new(DashMap::new()),
            range_indexes: Arc::new(DashMap::new()),
        }
    }

    pub fn create_index(&self, _index_name: &str, table_name: &str, column_expr: &str) -> Result<()> {
        if let Some(table_lock) = self.tables.get(table_name) {
            let table = table_lock.read().map_err(|_| anyhow!("Lock poison"))?;
            
            // Check if this is a JSON path index (column->path)
            let is_json_path = column_expr.contains("->");
            
            let col_idx = if is_json_path {
                // Extract base column name
                let arrow_pos = column_expr.find("->").unwrap();
                let base_col = &column_expr[..arrow_pos];
                table.columns.iter().position(|c| c.name == base_col)
                    .ok_or(anyhow!("Column not found: {}", base_col))?
            } else {
                table.columns.iter().position(|c| c.name == column_expr)
                    .ok_or(anyhow!("Column not found"))?
            };
            
            // Build Hash index
            let value_map: DashMap<UnifiedValue, Vec<u64>> = DashMap::new();
            // Build Range index (B-Tree)
            let mut range_map: BTreeMap<UnifiedValue, Vec<u64>> = BTreeMap::new();

            for (row_id, row) in &table.rows {
                let val = if is_json_path {
                    // Extract JSON path value
                    Self::extract_json_path_value(row, &table.columns, column_expr)
                        .unwrap_or(UnifiedValue::Null)
                } else {
                    row[col_idx].clone()
                };
                value_map.entry(val.clone()).or_insert_with(Vec::new).push(*row_id);
                range_map.entry(val.clone()).or_insert_with(Vec::new).push(*row_id);
            }
            
            // Store Hash Index (use full column expression as key for JSON paths)
            self.indexes
                .entry(table_name.to_string())
                .or_insert_with(DashMap::new)
                .insert(column_expr.to_string(), value_map);
            
            // Store Range Index
            self.range_indexes
                .entry(table_name.to_string())
                .or_insert_with(DashMap::new)
                .insert(column_expr.to_string(), RwLock::new(range_map));
            
            Ok(())
        } else {
            Err(anyhow!("Table not found"))
        }
    }
    
    /// Helper: Extract a value from a row using a JSON path expression
    fn extract_json_path_value(row: &Vec<UnifiedValue>, columns: &Vec<Column>, path_expr: &str) -> Option<UnifiedValue> {
        if let Some(arrow_pos) = path_expr.find("->") {
            let col_name = &path_expr[..arrow_pos];
            let json_path = &path_expr[arrow_pos..];
            
            let col_idx = columns.iter().position(|c| c.name == col_name)?;
            let col_type = &columns[col_idx].data_type;
            
            if !matches!(col_type, DataType::Json) {
                return None;
            }
            
            let mut current = row[col_idx].clone();
            let mut remaining = json_path;
            
            while !remaining.is_empty() {
                if remaining.starts_with("->>") {
                    remaining = &remaining[3..];
                } else if remaining.starts_with("->") {
                    remaining = &remaining[2..];
                } else {
                    break;
                }
                
                let key_end = remaining.find(|c: char| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(remaining.len());
                let key = &remaining[..key_end];
                remaining = &remaining[key_end..];
                
                match &current {
                    UnifiedValue::Object(map) => {
                        current = map.get(key).cloned().unwrap_or(UnifiedValue::Null);
                    },
                    UnifiedValue::Array(arr) => {
                        if let Ok(idx) = key.parse::<usize>() {
                            current = arr.get(idx).cloned().unwrap_or(UnifiedValue::Null);
                        } else {
                            return None;
                        }
                    },
                    _ => return None,
                }
            }
            
            Some(current)
        } else {
            None
        }
    }

    pub fn create_table(&self, name: String, columns: Vec<Column>) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(anyhow!("Table already exists"));
        }
        
        // Auto-create indices for Primary Keys
        // We do this by creating the table first, then calling create_index internally?
        // Or just setting up the structure.
        // For simplicity, we just init the table. Index creation usually happens explicitly or we can bootstrap it.
        // PLAN: Auto-index PKs.
        
        let table = Table {
            name: name.clone(),
            columns: columns.clone(),
            next_row_id: 1,
            rows: BTreeMap::new(),
        };
        
        // Insert table first
        self.tables.insert(name.clone(), RwLock::new(table));
        
        // Now create indices for PKs
        for col in columns {
            if col.is_primary_key {
                // Ignore error if fails (shouldn't fails on empty table)
                let _ = self.create_index(&format!("pk_{}_{}", name, col.name), &name, &col.name);
            }
        }
        
        Ok(())
    }

    pub fn insert(&self, table_name: &str, values: Vec<String>) -> Result<()> {
        if let Some(table_lock) = self.tables.get(table_name) {
            let mut table = table_lock.write().map_err(|_| anyhow!("Lock poison"))?;
            if values.len() != table.columns.len() {
                return Err(anyhow!("Column count mismatch"));
            }

            // Parse values to UnifiedValue
            let mut parsed_values = Vec::new();
            for (i, val_str) in values.iter().enumerate() {
                let col_type = &table.columns[i].data_type;
                let val = match col_type {
                    DataType::Integer => UnifiedValue::Integer(val_str.parse().unwrap_or(0)),
                    DataType::Float => UnifiedValue::Float(val_str.parse().unwrap_or(0.0)),
                    DataType::Boolean => UnifiedValue::Boolean(val_str.parse().unwrap_or(false)),
                    DataType::String => UnifiedValue::String(val_str.clone()),
                    DataType::DateTime => UnifiedValue::DateTime(val_str.parse().unwrap_or(0)),
                    DataType::Blob => UnifiedValue::Blob(val_str.clone()),
                    DataType::Json => {
                        // Parse JSON string into UnifiedValue
                        serde_json::from_str::<serde_json::Value>(val_str)
                            .map(|v| UnifiedValue::from(v))
                            .unwrap_or(UnifiedValue::Null)
                    },
                    DataType::Vector => {
                        if val_str.trim().starts_with('[') {
                            if let Ok(vec) = serde_json::from_str::<Vec<f64>>(val_str) {
                                UnifiedValue::Vector(vec)
                            } else {
                                // Try convert simple array of strings/nums?
                                // Fallback: Null or Error.
                                // Let's return Null if invalid vector format.
                                UnifiedValue::Null 
                            }
                        } else {
                            UnifiedValue::Null
                        }
                    },
                };
                parsed_values.push(val);
            }

            // Check Primary Key Uniqueness (O(1) via Index)
            if let Some(pk_idx) = table.columns.iter().position(|c| c.is_primary_key) {
                let pk_val = &parsed_values[pk_idx];
                let pk_col_name = &table.columns[pk_idx].name;
                
                // Look up in index
                if let Some(table_indexes) = self.indexes.get(table_name) {
                     if let Some(col_index) = table_indexes.get(pk_col_name) {
                         if col_index.contains_key(pk_val) {
                             return Err(anyhow!("Constraint violation: Duplicate primary key '{}'", pk_val));
                         }
                     }
                }
            }

            // Check Foreign Key Constraints (O(1) via Index)
            for (i, col) in table.columns.iter().enumerate() {
                if let Some((ref ref_table_name, ref ref_col_name)) = col.references {
                    let val = &parsed_values[i];
                    
                    // Verify that the referenced value exists in the referenced table's index
                    let exists = if let Some(ref_indexes) = self.indexes.get(ref_table_name) {
                        if let Some(ref_col_index) = ref_indexes.get(ref_col_name) {
                            ref_col_index.contains_key(val)
                        } else {
                            // Reference column not indexed? Fallback or Error.
                            return Err(anyhow!("Referenced column '{}.{}' is not indexed. FK targets must be indexed.", ref_table_name, ref_col_name));
                        }
                    } else {
                         return Err(anyhow!("Referenced table '{}' not found", ref_table_name));
                    };

                    if !exists {
                         return Err(anyhow!("Constraint violation: FK '{}' not found in '{}.{}'", val, ref_table_name, ref_col_name));
                    }
                }
            }

            let row_id = table.next_row_id;
            table.next_row_id += 1;
            
            table.rows.insert(row_id, parsed_values.clone());
            
            // Maintain indexes
            drop(table); // Release read lock
            
            // 1. Maintain Hash Indexes
            if let Some(table_indexes) = self.indexes.get(table_name) {
                let table_lock = self.tables.get(table_name).unwrap();
                let table = table_lock.read().map_err(|_| anyhow!("Lock poison"))?;
                
                for col_entry in table_indexes.iter() {
                    let col_name = col_entry.key();
                    if let Some(col_idx) = table.columns.iter().position(|c| &c.name == col_name) {
                        let val = &parsed_values[col_idx];
                        col_entry.value().entry(val.clone()).or_insert_with(Vec::new).push(row_id);
                    }
                }
            }

            // 2. Maintain Range Indexes (B-Tree)
            if let Some(table_range_indexes) = self.range_indexes.get(table_name) {
                let table_lock = self.tables.get(table_name).unwrap();
                let table = table_lock.read().map_err(|_| anyhow!("Lock poison"))?;

                for col_entry in table_range_indexes.iter() {
                    let col_name = col_entry.key();
                    if let Some(col_idx) = table.columns.iter().position(|c| &c.name == col_name) {
                        let val = &parsed_values[col_idx];
                        let mut btree = col_entry.value().write().map_err(|_| anyhow!("Lock poison"))?;
                        btree.entry(val.clone()).or_insert_with(Vec::new).push(row_id);
                    }
                }
            }
            
            Ok(())
        } else {
            Err(anyhow!("Table not found"))
        }
    }





    fn evaluate_condition(&self, row_val: &UnifiedValue, target_val: &str, col_type: &DataType, op: &Operator) -> bool {
        // Parse target_val to UnifiedValue for comparison
        let target = match col_type {
            DataType::Integer => UnifiedValue::Integer(target_val.parse().unwrap_or(0)),
            DataType::Float => UnifiedValue::Float(target_val.parse().unwrap_or(0.0)),
            DataType::Boolean => UnifiedValue::Boolean(target_val.parse().unwrap_or(false)),
            DataType::String => UnifiedValue::String(target_val.to_string()),
            DataType::DateTime => UnifiedValue::DateTime(target_val.parse().unwrap_or(0)),
            DataType::Blob => UnifiedValue::Blob(target_val.to_string()),
            DataType::Json => serde_json::from_str::<serde_json::Value>(target_val)
                .map(UnifiedValue::from)
                .unwrap_or(UnifiedValue::Null),
            DataType::Vector => UnifiedValue::Null, 
        };

        match op {
            Operator::Eq => row_val == &target,
            Operator::Neq => row_val != &target,
            Operator::Gt => row_val > &target,
            Operator::Lt => row_val < &target,
            Operator::Gte => row_val >= &target,
            Operator::Lte => row_val <= &target,
            Operator::Like => {
                if let (UnifiedValue::String(s), UnifiedValue::String(p)) = (row_val, &target) {
                     let pattern = p.replace('%', ".*").replace('_', ".");
                     regex::Regex::new(&format!("^{}$", pattern))
                        .map(|re| re.is_match(s))
                        .unwrap_or(false)
                } else {
                    false
                }
            },
            Operator::In => {
                // target_val is a comma-separated string, we need to parse each part
                let parts: Vec<&str> = target_val.split(',').collect();
                parts.iter().any(|part| {
                     let t = match col_type {
                        DataType::Integer => UnifiedValue::Integer(part.parse().unwrap_or(0)),
                        DataType::Float => UnifiedValue::Float(part.parse().unwrap_or(0.0)),
                        DataType::Boolean => UnifiedValue::Boolean(part.parse().unwrap_or(false)),
                        DataType::String => UnifiedValue::String(part.to_string()),
                        DataType::DateTime => UnifiedValue::DateTime(part.parse().unwrap_or(0)),
                        DataType::Blob => UnifiedValue::Blob(part.to_string()),
                        DataType::Json => serde_json::from_str::<serde_json::Value>(part)
                            .map(UnifiedValue::from)
                            .unwrap_or(UnifiedValue::Null),
                        DataType::Vector => UnifiedValue::Null,
                    };
                    row_val == &t
                })
            }
        }
    }

    /// Resolve a JSON path expression like "column->field->nested" into a value
    fn resolve_json_path(&self, row: &Vec<UnifiedValue>, columns: &Vec<Column>, path_expr: &str) -> Option<(UnifiedValue, DataType)> {
        // Check if path contains arrow operator
        if let Some(arrow_pos) = path_expr.find("->") {
            let col_name = &path_expr[..arrow_pos];
            let json_path = &path_expr[arrow_pos..];
            
            // Find the column
            let col_idx = columns.iter().position(|c| c.name == col_name)?;
            let col_type = &columns[col_idx].data_type;
            
            // Only JSON columns support path access
            if !matches!(col_type, DataType::Json) {
                return None;
            }
            
            let mut current = row[col_idx].clone();
            
            // Parse path segments (split by -> or ->>)
            let path_str = json_path;
            let mut remaining = path_str;
            
            while !remaining.is_empty() {
                // Skip leading arrow
                if remaining.starts_with("->>") {
                    remaining = &remaining[3..];
                } else if remaining.starts_with("->") {
                    remaining = &remaining[2..];
                } else {
                    break;
                }
                
                // Extract key (alphanumeric)
                let key_end = remaining.find(|c: char| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(remaining.len());
                let key = &remaining[..key_end];
                remaining = &remaining[key_end..];
                
                // Navigate into JSON
                match &current {
                    UnifiedValue::Object(map) => {
                        current = map.get(key).cloned().unwrap_or(UnifiedValue::Null);
                    },
                    UnifiedValue::Array(arr) => {
                        // Support numeric index access
                        if let Ok(idx) = key.parse::<usize>() {
                            current = arr.get(idx).cloned().unwrap_or(UnifiedValue::Null);
                        } else {
                            return None;
                        }
                    },
                    _ => return None,
                }
            }
            
            // Determine result type based on the extracted value
            let result_type = match &current {
                UnifiedValue::Integer(_) => DataType::Integer,
                UnifiedValue::Float(_) => DataType::Float,
                UnifiedValue::Boolean(_) => DataType::Boolean,
                UnifiedValue::String(_) => DataType::String,
                _ => DataType::Json,
            };
            
            Some((current, result_type))
        } else {
            // Simple column reference
            let col_idx = columns.iter().position(|c| c.name == path_expr)?;
            Some((row[col_idx].clone(), columns[col_idx].data_type.clone()))
        }
    }

    fn evaluate_filter(&self, filter: &Filter, row: &Vec<UnifiedValue>, columns: &Vec<Column>) -> bool {
        match filter {
            Filter::Condition(col_expr, op, val) => {
                // Resolve the column expression (may include ->path)
                if let Some((row_val, col_type)) = self.resolve_json_path(row, columns, col_expr) {
                    self.evaluate_condition(&row_val, val, &col_type, op)
                } else {
                    false 
                }
            },
            Filter::And(left, right) => {
                self.evaluate_filter(left, row, columns) && self.evaluate_filter(right, row, columns)
            },
            Filter::Or(left, right) => {
                self.evaluate_filter(left, row, columns) || self.evaluate_filter(right, row, columns)
            }
        }
    }

    fn get_optimized_indices(&self, table_name: &str, filter: &Filter) -> Option<Vec<u64>> {
        match filter {
            Filter::Condition(col, op, val) => {
                 // JSON path expressions (column->path) - check for JSON path index
                 let is_json_path = col.contains("->");
                 
                 // Determine the target value type
                 let target = if is_json_path {
                     // For JSON paths, parse the target value based on what it looks like
                     // Since we extract the actual type from JSON, we need to parse accordingly
                     if let Ok(i) = val.parse::<i64>() {
                         UnifiedValue::Integer(i)
                     } else if let Ok(f) = val.parse::<f64>() {
                         UnifiedValue::Float(f)
                     } else if val == "true" || val == "false" {
                         UnifiedValue::Boolean(val.parse().unwrap_or(false))
                     } else {
                         UnifiedValue::String(val.to_string())
                     }
                 } else {
                     // Regular column - get type from table schema
                     let col_type = if let Some(table_lock) = self.tables.get(table_name) {
                         if let Ok(table) = table_lock.read() {
                             table.columns.iter().find(|c| c.name == *col)?.data_type.clone()
                         } else { return None; }
                     } else { return None; };

                     match col_type {
                        DataType::Integer => UnifiedValue::Integer(val.parse().unwrap_or(0)),
                        DataType::Float => UnifiedValue::Float(val.parse().unwrap_or(0.0)),
                        DataType::Boolean => UnifiedValue::Boolean(val.parse().unwrap_or(false)),
                        DataType::String => UnifiedValue::String(val.to_string()),
                        DataType::DateTime => UnifiedValue::DateTime(val.parse().unwrap_or(0)),
                        DataType::Blob => UnifiedValue::Blob(val.to_string()),
                        DataType::Json => serde_json::from_str::<serde_json::Value>(val)
                            .map(UnifiedValue::from)
                            .unwrap_or(UnifiedValue::Null),
                        DataType::Vector => UnifiedValue::Null,
                    }
                 };

                // a. Try Equality Index (Hash) - works for both regular and JSON path
                if matches!(op, Operator::Eq) {
                    if let Some(table_indexes) = self.indexes.get(table_name) {
                        if let Some(col_index) = table_indexes.get(col) {
                            if let Some(row_indices) = col_index.get(&target) {
                                return Some(row_indices.clone());
                            } else {
                                return Some(Vec::new());
                            }
                        }
                    }
                }
                
                // b. Try Range Index (B-Tree) - works for both regular and JSON path
                if matches!(op, Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte) {
                    if let Some(table_ranges) = self.range_indexes.get(table_name) {
                        if let Some(col_range_lock) = table_ranges.get(col) {
                            if let Ok(btree) = col_range_lock.read() {
                                let row_indices: Vec<u64> = match op {
                                    Operator::Gt => btree.range((Excluded(target), Unbounded)).flat_map(|(_, v)| v).cloned().collect(),
                                    Operator::Gte => btree.range((Included(target), Unbounded)).flat_map(|(_, v)| v).cloned().collect(),
                                    Operator::Lt => btree.range((Unbounded, Excluded(target))).flat_map(|(_, v)| v).cloned().collect(),
                                    Operator::Lte => btree.range((Unbounded, Included(target))).flat_map(|(_, v)| v).cloned().collect(),
                                    _ => unreachable!(),
                                };
                                return Some(row_indices);
                            }
                        }
                    }
                }
                None
            }
            Filter::And(left, right) => {
                let left_indices = self.get_optimized_indices(table_name, left);
                let right_indices = self.get_optimized_indices(table_name, right);
                
                match (left_indices, right_indices) {
                    (Some(l), Some(r)) => {
                        let r_set: std::collections::HashSet<u64> = r.into_iter().collect();
                        Some(l.into_iter().filter(|i| r_set.contains(i)).collect())
                    }
                    (Some(l), None) => Some(l),
                    (None, Some(r)) => Some(r),
                    (None, None) => None,
                }
            }
            Filter::Or(left, right) => {
                let left_indices = self.get_optimized_indices(table_name, left);
                let right_indices = self.get_optimized_indices(table_name, right);
                
                match (left_indices, right_indices) {
                    (Some(l), Some(r)) => {
                        let mut set: std::collections::HashSet<u64> = l.into_iter().collect();
                        set.extend(r);
                        Some(set.into_iter().collect())
                    }
                    _ => None,
                }
            }
        }
    }

    pub fn select(
        &self, 
        table_name: &str, 
        selector: Selector,
        join: Option<Vec<JoinClause>>,
        filter: Option<Filter>,
        group_by: Option<Vec<String>>,
        having: Option<Filter>,
        order_by: Option<(String, bool)>,
        limit: Option<usize>,
        offset: Option<usize>
    ) -> Result<Vec<Vec<String>>> {
        if let Some(ref joins) = join {
            if !joins.is_empty() {
                return self.select_joined(table_name, selector, joins, filter, group_by, having, order_by, limit, offset);
            }
        }

        if let Some(table_lock) = self.tables.get(table_name) {
            let table = table_lock.read().map_err(|_| anyhow!("Lock poison"))?;
            
            // 1. Filter (WHERE) - Try optimized index traversal
            let mut rows: Vec<Vec<UnifiedValue>> = if let Some(ref f) = filter {
                if let Some(row_indices) = self.get_optimized_indices(table_name, f) {
                    // Use optimized candidates
                    row_indices.iter()
                        .filter_map(|&id| table.rows.get(&id))
                        .filter(|row| self.evaluate_filter(f, row, &table.columns))
                        .cloned()
                        .collect()
                } else {
                    // Fall back to full scan
                    table.rows.values()
                        .filter(|row| self.evaluate_filter(f, row, &table.columns))
                        .cloned()
                        .collect()
                }
            } else {
                table.rows.values().cloned().collect()
            };

            // 2. Grouping & Aggregation
            let is_aggregate_selector = matches!(selector, Selector::Count | Selector::Sum(_) | Selector::Avg(_) | Selector::Max(_) | Selector::Min(_));
            
            if let Some(ref group_cols) = group_by {
                // Determine indices of grouping columns
                let mut group_indices = Vec::new();
                for col in group_cols {
                    if let Some(idx) = table.columns.iter().position(|c| c.name == *col) {
                        group_indices.push(idx);
                    } else {
                        return Err(anyhow!("Group column '{}' not found", col));
                    }
                }

                // Partition into buckets
                let mut buckets: std::collections::HashMap<Vec<UnifiedValue>, Vec<Vec<UnifiedValue>>> = std::collections::HashMap::new();
                
                for row in rows {
                    let key: Vec<UnifiedValue> = group_indices.iter().map(|&i| row[i].clone()).collect();
                    buckets.entry(key).or_insert_with(Vec::new).push(row);
                }

                // Aggregate each bucket
                rows = Vec::new();
                for (key, bucket_rows) in buckets {
                    let agg_val = self.compute_aggregate(&selector, &bucket_rows, &table.columns)?;
                    // Result Row schema: [Group Col 1, Group Col 2, ..., Aggregate Value]
                    let mut res_row = key;
                    res_row.push(agg_val);
                    rows.push(res_row);
                }

                // HAVING: Filter aggregated results
                if let Some(having_filter) = having {
                    // HAVING filters on the aggregated column (last column in result row)
                    let agg_col_idx = rows.first().map(|r| r.len().saturating_sub(1)).unwrap_or(0);
                    rows.retain(|row| {
                        if let Some(agg_val) = row.get(agg_col_idx) {
                             // Create a temporary column definition for the aggregate value
                             // We assume it's a Number (Int or Float) for now based on aggregation
                            let agg_type = match agg_val {
                                UnifiedValue::Integer(_) => DataType::Integer,
                                UnifiedValue::Float(_) => DataType::Float,
                                _ => DataType::String,
                             };
                            
                            match &having_filter {
                                Filter::Condition(_, op, value) => {
                                    self.evaluate_condition(agg_val, value, &agg_type, op)
                                }
                                _ => true, // Complex filters not supported in HAVING yet
                            }
                        } else {
                            false
                        }
                    });
                }

            } else if is_aggregate_selector {
                // Global aggregation
                let agg_val = self.compute_aggregate(&selector, &rows, &table.columns)?;
                rows = vec![vec![agg_val]];
            }

            // 3. Order
            if !is_aggregate_selector && group_by.is_none() {
                 if let Some((col_name, ascending)) = order_by {
                    if let Some(col_idx) = table.columns.iter().position(|c| c.name == col_name) {
                        rows.sort_by(|a, b| {
                            let cmp = a[col_idx].cmp(&b[col_idx]);
                            if ascending { cmp } else { cmp.reverse() }
                        });
                    }
                }
            }

            // 4. Offset
            if let Some(n) = offset {
                rows = rows.into_iter().skip(n).collect();
            }

            // 5. Limit
            if let Some(n) = limit {
                rows.truncate(n);
            }
            
            // Format to String for return
            let mut string_rows = Vec::new();
            for row in rows {
                if is_aggregate_selector || group_by.is_some() {
                     string_rows.push(row.iter().map(|v| v.to_string()).collect());
                } else {
                    match &selector {
                        Selector::All => {
                             string_rows.push(row.iter().map(|v| v.to_string()).collect());
                        },
                        Selector::Columns(cols) => {
                             let mut proj = Vec::new();
                             for col_name in cols {
                                 let clean_name = if let Some(pos) = col_name.find('.') { &col_name[pos+1..] } else { col_name };
                                 if let Some(idx) = table.columns.iter().position(|c| c.name == clean_name) {
                                     proj.push(row[idx].to_string());
                                 } else {
                                      return Err(anyhow!("Column '{}' not found", col_name));
                                 }
                             }
                             string_rows.push(proj);
                        },
                        _ => string_rows.push(row.iter().map(|v| v.to_string()).collect()) 
                    }
                }
            }

            Ok(string_rows)
        } else {
            Err(anyhow!("Table not found"))
        }
    }

    pub fn alter_table(&self, table_name: &str, op: AlterOp) -> Result<()> {
        if let Some(table_lock) = self.tables.get(table_name) {
            let mut table = table_lock.write().map_err(|_| anyhow!("Lock poison"))?;
            
            match op {
                AlterOp::Add(col_name, col_type_str) => {
                    // Check if column exists
                    if table.columns.iter().any(|c| c.name == col_name) {
                        return Err(anyhow!("Column '{}' already exists", col_name));
                    }
                    
                    let data_type = match col_type_str.to_uppercase().as_str() {
                        "INT" | "INTEGER" => DataType::Integer,
                        "BOOL" | "BOOLEAN" => DataType::Boolean,
                        "FLOAT" | "DOUBLE" => DataType::Float,
                        "DATETIME" | "TIMESTAMP" => DataType::DateTime,
                        "BLOB" | "BYTES" => DataType::Blob,
                        "JSON" => DataType::Json,
                        _ => DataType::String,
                    };

                    // Add Column
                    table.columns.push(Column {
                        name: col_name,
                        data_type: data_type.clone(),
                        is_primary_key: false, // Cannot add PK via ALTER
                        references: None,      // Simple ADD for now
                    });

                    // Backfill Rows
                    let default_val = match data_type {
                        DataType::Integer => UnifiedValue::Integer(0),
                        DataType::Boolean => UnifiedValue::Boolean(false),
                        DataType::Float => UnifiedValue::Float(0.0),
                        DataType::DateTime => UnifiedValue::DateTime(0),
                        DataType::String => UnifiedValue::String("".to_string()),
                        DataType::Blob => UnifiedValue::Blob("".to_string()),
                        DataType::Json => UnifiedValue::Null,
                        DataType::Vector => UnifiedValue::Null,
                    };
                    
                    for row in table.rows.values_mut() {
                        row.push(default_val.clone());
                    }
                },
                AlterOp::Drop(col_name) => {
                    if let Some(idx) = table.columns.iter().position(|c| c.name == col_name) {
                        // Prevent dropping PK
                        if table.columns[idx].is_primary_key {
                            return Err(anyhow!("Cannot drop primary key column"));
                        }
                        
                        // Remove Column
                        table.columns.remove(idx);
                        
                        // Remove Data
                        for row in table.rows.values_mut() {
                            if idx < row.len() {
                                row.remove(idx);
                            }
                        }
                    } else {
                        return Err(anyhow!("Column '{}' not found", col_name));
                    }
                }
            }
            Ok(())
        } else {
            Err(anyhow!("Table not found"))
        }
    }

    pub fn update(&self, table_name: &str, filter: Option<Filter>, set: (String, String)) -> Result<()> {
        if let Some(table_lock) = self.tables.get(table_name) {
            let mut table = table_lock.write().map_err(|_| anyhow!("Lock poison"))?;
            
            let (set_col, set_val) = set;
            let set_idx = table.columns.iter().position(|c| c.name == set_col)
                .ok_or(anyhow!("Set column not found"))?;
            
            let columns = table.columns.clone();

            // Pre-calculate new value
            let col_type = &columns[set_idx].data_type;
            let new_val = match col_type {
                DataType::Integer => UnifiedValue::Integer(set_val.parse().unwrap_or(0)),
                DataType::Float => UnifiedValue::Float(set_val.parse().unwrap_or(0.0)),
                DataType::Boolean => UnifiedValue::Boolean(set_val.parse().unwrap_or(false)),
                DataType::String => UnifiedValue::String(set_val.clone()),
                DataType::DateTime => UnifiedValue::DateTime(set_val.parse().unwrap_or(0)),
                DataType::Blob => UnifiedValue::Blob(set_val.clone()),
                DataType::Json => serde_json::from_str::<serde_json::Value>(&set_val)
                    .map(UnifiedValue::from)
                    .unwrap_or(UnifiedValue::Null),
                DataType::Vector => UnifiedValue::Null, // Update vector via string? Maybe later.
            };

            // Identify rows to update
            let mut ids_to_update = Vec::new();
            for (id, row) in &table.rows {
                 let matches = if let Some(ref f) = filter {
                    self.evaluate_filter(f, row, &columns)
                } else {
                    true 
                };
                if matches {
                    ids_to_update.push(*id);
                }
            }
            
            for id in ids_to_update {
                if let Some(row) = table.rows.get_mut(&id) {
                    let old_val = row[set_idx].clone();
                    // Update value
                    row[set_idx] = new_val.clone();
                    
                    // Maintain Hash Indexes
                    if let Some(table_indexes) = self.indexes.get(table_name) {
                        if let Some(col_index) = table_indexes.get(&set_col) {
                            // Remove from old
                            if let Some(mut rows_vec) = col_index.get_mut(&old_val) {
                                rows_vec.retain(|&x| x != id);
                            }
                            // Add to new
                            col_index.entry(new_val.clone()).or_insert_with(Vec::new).push(id);
                        }
                    }
                     // Maintain Range Indexes
                    if let Some(table_ranges) = self.range_indexes.get(table_name) {
                         if let Some(col_range) = table_ranges.get(&set_col) {
                             if let Ok(mut btree) = col_range.write() {
                                 // Remove from old
                                 if let Some(rows_vec) = btree.get_mut(&old_val) {
                                     rows_vec.retain(|&x| x != id);
                                 }
                                 // Add to new
                                 btree.entry(new_val.clone()).or_insert_with(Vec::new).push(id);
                             }
                        }
                    }
                }
            }
            Ok(())
        } else {
             Err(anyhow!("Table not found"))
        }
    }

    pub fn delete(&self, table_name: &str, filter: Option<Filter>) -> Result<()> {
        if let Some(table_lock) = self.tables.get(table_name) {
            let mut table = table_lock.write().map_err(|_| anyhow!("Lock poison"))?;
            let columns = table.columns.clone();

            // 1. Find IDs to delete
            let mut ids_to_delete = Vec::new();
            for (id, row) in &table.rows {
                if let Some(ref f) = filter {
                     if self.evaluate_filter(f, row, &columns) {
                         ids_to_delete.push(*id);
                     }
                } else {
                    // No filter = delete all
                    ids_to_delete.push(*id);
                }
            }

            // 2. Delete and Update Indices
            for id in ids_to_delete {
                if let Some(row) = table.rows.remove(&id) {
                    // Maintain Hash Indexes
                    if let Some(table_indexes) = self.indexes.get(table_name) {
                         for col_entry in table_indexes.iter() {
                             let col_name = col_entry.key();
                             if let Some(col_idx) = columns.iter().position(|c| &c.name == col_name) {
                                  let val = &row[col_idx];
                                  if let Some(mut rows_vec) = col_entry.value().get_mut(val) {
                                      rows_vec.retain(|&x| x != id);
                                  }
                             }
                         }
                    }
                    
                    // Maintain Range Indexes
                     if let Some(table_ranges) = self.range_indexes.get(table_name) {
                         for col_entry in table_ranges.iter() {
                             let col_name = col_entry.key();
                             if let Some(col_idx) = columns.iter().position(|c| &c.name == col_name) {
                                  let val = &row[col_idx];
                                  if let Ok(mut btree) = col_entry.value().write() {
                                       if let Some(rows_vec) = btree.get_mut(val) {
                                           rows_vec.retain(|&x| x != id);
                                       }
                                  }
                             }
                         }
                    }
                }
            }
            Ok(())
        } else {
            Err(anyhow!("Table not found"))
        }
    }

    fn compute_aggregate(&self, selector: &Selector, rows: &Vec<Vec<UnifiedValue>>, columns: &Vec<Column>) -> Result<UnifiedValue> {
        match selector {
            Selector::Count => Ok(UnifiedValue::Integer(rows.len() as i64)),
            Selector::Sum(col) | Selector::Avg(col) | Selector::Max(col) | Selector::Min(col) => {
                 let col_idx = columns.iter().position(|c| c.name == *col)
                    .ok_or(anyhow!("Aggregate column not found"))?;
                
                 let mut nums: Vec<f64> = Vec::new();
                 let mut ints: Vec<i64> = Vec::new();
                 let mut all_ints = true;

                 for r in rows {
                     match r[col_idx] {
                         UnifiedValue::Integer(i) => ints.push(i),
                         UnifiedValue::Float(f) => {
                             all_ints = false;
                             nums.push(f);
                         }
                         _ => {}
                     }
                 }

                 if !all_ints {
                     nums.extend(ints.iter().map(|&i| i as f64));
                 }

                 match selector {
                     Selector::Sum(_) => {
                         if all_ints { Ok(UnifiedValue::Integer(ints.iter().sum())) } else { Ok(UnifiedValue::Float(nums.iter().sum())) }
                     },
                     Selector::Avg(_) => {
                         if all_ints {
                             let count = ints.len() as f64;
                             if count == 0.0 { Ok(UnifiedValue::Float(0.0)) } else { Ok(UnifiedValue::Float(ints.iter().sum::<i64>() as f64 / count)) }
                         } else {
                             let count = nums.len() as f64;
                             if count == 0.0 { Ok(UnifiedValue::Float(0.0)) } else { Ok(UnifiedValue::Float(nums.iter().sum::<f64>() / count)) }
                         }
                     },
                     Selector::Max(_) => {
                         // Re-scan for Max (or sort?)
                         let max = rows.iter().map(|r| &r[col_idx]).max();
                         Ok(max.cloned().unwrap_or(UnifiedValue::Null))
                     },
                     Selector::Min(_) => {
                         let min = rows.iter().map(|r| &r[col_idx]).min();
                         Ok(min.cloned().unwrap_or(UnifiedValue::Null))
                     },
                     _ => unreachable!()
                 }
            },
            Selector::All | Selector::Columns(_) => Err(anyhow!("Cannot aggregate with * or list")),
        }
    }

    fn select_joined(
        &self,
        table_name: &str,
        selector: Selector,
        joins: &Vec<JoinClause>,
        filter: Option<Filter>,
        group_by: Option<Vec<String>>,
        having: Option<Filter>,
        _order_by: Option<(String, bool)>, 
        limit: Option<usize>,
        offset: Option<usize>
    ) -> Result<Vec<Vec<String>>> {
        let mut rows = self.scan_table_map(table_name)?;
        
        for join in joins {
            let right_rows = self.scan_table_map(&join.table)?;
            let mut joined = Vec::new();
            
            for l_row in &rows {
                for r_row in &right_rows {
                    let l_val = self.resolve_val_map(l_row, &join.on_left);
                    let r_val = self.resolve_val_map(r_row, &join.on_right);
                    
                    if l_val == r_val && l_val != UnifiedValue::Null {
                        let mut new_row = l_row.clone();
                        new_row.extend(r_row.clone());
                        joined.push(new_row);
                    }
                }
            }
            rows = joined;
        }
        
        if let Some(f) = filter {
            rows.retain(|row| self.evaluate_filter_map(&f, row));
        }
        
        let is_aggregate_selector = !matches!(selector, Selector::All | Selector::Columns(_));

        // GROUP BY Logic for JOINs
        if let Some(group_cols) = group_by {
            let mut buckets: HashMap<Vec<UnifiedValue>, Vec<HashMap<String, UnifiedValue>>> = HashMap::new();
            for row in rows {
                let key: Vec<UnifiedValue> = group_cols.iter()
                    .map(|c| self.resolve_val_map(&row, c))
                    .collect();
                buckets.entry(key).or_insert_with(Vec::new).push(row);
            }

            let mut agg_results = Vec::new();
            for (key, bucket_rows) in buckets {
                // Compute aggregate using map values
                let agg_val = self.compute_aggregate_map(&selector, &bucket_rows)?;
                
                // Check HAVING
                let mut matches_having = true;
                if let Some(ref h_filter) = having {
                    match h_filter {
                        Filter::Condition(_, op, val_str) => {
                             // Simplification: HAVING on aggregate value (last column)
                             matches_having = self.evaluate_condition(&agg_val, val_str, &DataType::Float, op);
                        },
                        _ => {}
                    }
                }

                if matches_having {
                    let mut res_row: Vec<String> = key.iter().map(|v| v.to_string()).collect();
                    res_row.push(agg_val.to_string());
                    agg_results.push(res_row);
                }
            }
            return Ok(self.apply_limit_offset(agg_results, limit, offset));

        } else if is_aggregate_selector {
            // Global aggregation over joined rows
            let agg_val = self.compute_aggregate_map(&selector, &rows)?;
            return Ok(vec![vec![agg_val.to_string()]]);
        }

        let mut results = Vec::new();
        for row in rows {
            match &selector {
                Selector::Columns(cols) => {
                    let mut proj = Vec::new();
                    for col in cols {
                         proj.push(self.resolve_val_map(&row, col).to_string());
                    }
                    results.push(proj);
                },
                Selector::All => {
                    // Collect values from map, prefer qualified names? 
                    // To keep it simple and stable, let's just return values.
                    results.push(row.values().map(|v| v.to_string()).collect());
                },
                _ => unreachable!() 
            }
        }
        
        Ok(self.apply_limit_offset(results, limit, offset))
    }

    fn compute_aggregate_map(&self, selector: &Selector, rows: &Vec<HashMap<String, UnifiedValue>>) -> Result<UnifiedValue> {
        match selector {
            Selector::Count => Ok(UnifiedValue::Integer(rows.len() as i64)),
            Selector::Sum(col) | Selector::Avg(col) | Selector::Max(col) | Selector::Min(col) => {
                 let mut nums: Vec<f64> = Vec::new();
                 for r in rows {
                     let val = self.resolve_val_map(r, col);
                     match val {
                         UnifiedValue::Integer(i) => nums.push(i as f64),
                         UnifiedValue::Float(f) => nums.push(f),
                         _ => {}
                     }
                 }
                 
                 match selector {
                     Selector::Sum(_) => Ok(UnifiedValue::Float(nums.iter().sum())),
                     Selector::Avg(_) => {
                         let count = nums.len() as f64;
                         if count == 0.0 { Ok(UnifiedValue::Float(0.0)) } else { Ok(UnifiedValue::Float(nums.iter().sum::<f64>() / count)) }
                     },
                     Selector::Max(_) => Ok(UnifiedValue::Float(nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max))),
                     Selector::Min(_) => Ok(UnifiedValue::Float(nums.iter().cloned().fold(f64::INFINITY, f64::min))),
                     _ => unreachable!()
                 }
            },
            _ => Err(anyhow!("Invalid aggregate selector"))
        }
    }

    fn apply_limit_offset(&self, rows: Vec<Vec<String>>, limit: Option<usize>, offset: Option<usize>) -> Vec<Vec<String>> {
        let start = offset.unwrap_or(0);
        if start >= rows.len() { return Vec::new(); }
        
        let end = if let Some(l) = limit {
            (start + l).min(rows.len())
        } else {
            rows.len()
        };
        
        rows[start..end].to_vec()
    }


    fn scan_table_map(&self, table_name: &str) -> Result<Vec<HashMap<String, UnifiedValue>>> {
        if let Some(lock) = self.tables.get(table_name) {
            let table = lock.read().map_err(|_| anyhow!("Lock poison"))?;
            let mut res = Vec::new();
            for row_vals in table.rows.values() {
                let mut map = HashMap::new();
                for (i, col) in table.columns.iter().enumerate() {
                    map.insert(format!("{}.{}", table.name, col.name), row_vals[i].clone());
                    map.insert(col.name.clone(), row_vals[i].clone()); 
                }
                res.push(map);
            }
            Ok(res)
        } else {
            Err(anyhow!("Table {} not found", table_name))
        }
    }
    
    fn resolve_val_map(&self, row: &HashMap<String, UnifiedValue>, col: &str) -> UnifiedValue {
        if let Some(v) = row.get(col) {
            return v.clone();
        }
        // Try arrow?
        if let Some(pos) = col.find("->") {
             let base = &col[0..pos];
             // let path = &col[pos+2..]; 
             // Logic to extract JSON would go here. 
             // For now, MVP returns Null if not simple column or if path resolution fails
             if let Some(_) = row.get(base) {
                 return UnifiedValue::Null; 
             }
        }
        UnifiedValue::Null
    }

    fn evaluate_filter_map(&self, filter: &Filter, row: &HashMap<String, UnifiedValue>) -> bool {
        match filter {
            Filter::Condition(col, _op, val_str) => {
                let val = self.resolve_val_map(row, col);
                let target = UnifiedValue::String(val_str.clone()); 
                val == target 
            },
            Filter::And(l, r) => self.evaluate_filter_map(l, row) && self.evaluate_filter_map(r, row),
            Filter::Or(l, r) => self.evaluate_filter_map(l, row) || self.evaluate_filter_map(r, row),
        }
    }

    pub fn restore(&self, tables: HashMap<String, Table>) {
        self.tables.clear();
        self.indexes.clear();
        self.range_indexes.clear();
        
        for (name, table) in tables {
            let idx_cols: Vec<String> = table.columns.iter()
                .filter(|c| c.is_primary_key)
                .map(|c| c.name.clone())
                .collect();

            self.tables.insert(name.clone(), RwLock::new(table));
            
            for col in idx_cols {
                let _ = self.create_index(&name, &col, "HASH");
            }
        }
    }

    pub fn vector_search(&self, table_name: &str, col_name: &str, query: &Vec<f64>, limit: usize) -> Result<Vec<String>> {
        if let Some(table_lock) = self.tables.get(table_name) {
            let table = table_lock.read().map_err(|_| anyhow!("Lock poison"))?;
            
            let col_idx = table.columns.iter().position(|c| c.name == col_name)
                .ok_or(anyhow!("Column not found"))?;
            
            // Collect (similarity, row)
            let mut candidates: Vec<(f64, &Vec<UnifiedValue>)> = Vec::new();
            
            let query_val = UnifiedValue::Vector(query.clone());

            for row in table.rows.values() {
                let vec_val = &row[col_idx];
                // Type check handled by cosine_similarity logic (returns None if mismatch)
                if let Some(score) = vec_val.cosine_similarity(&query_val) {
                    candidates.push((score, row));
                }
            }
            
            // Sort Descending (Higher similarity first)
            candidates.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
            
            // Take limit
            let results = candidates.into_iter()
                .take(limit)
                .map(|(score, row)| {
                    // Format row
                    let vals: Vec<String> = row.iter().map(|v| v.to_string()).collect();
                    // Append Score? Or pure row?
                    // Let's return pure row for compatibility with select *
                    // But usually search needs score. 
                    // Let's valid JSON format for output?
                    // Executor expects Vec<String> -> displayed as internal strings
                    // I'll return SPACE separated for now, maybe with score prepended?
                    // "(score: 0.99) id name ..."
                    let row_str = vals.join(" ");
                    format!("(score: {:.4}) {}", score, row_str) 
                })
                .collect();
            
            Ok(results)
        } else {
            Err(anyhow!("Table not found"))
        }
    }
}
