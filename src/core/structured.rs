use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use std::collections::BTreeMap;
use std::ops::Bound::{Included, Excluded, Unbounded};
use anyhow::{Result, anyhow};
use crate::query::{Operator, Filter, Selector, AlterOp};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    String,
    Boolean,
    Float,
    DateTime, // Stored as ISO8601 string
    Blob,     // Stored as Base64 string
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub references: Option<(String, String)>, // (table, column)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<String>>, // Simplified storage as strings for MVP
}

// Derive removed
#[derive(Clone)]
pub struct StructuredStore {
    tables: Arc<DashMap<String, RwLock<Table>>>,
    // indexes: table_name -> (col_name -> (value -> row_indices))
    indexes: Arc<DashMap<String, DashMap<String, DashMap<String, Vec<usize>>>>>,
    // range_indexes: table_name -> (col_name -> BTreeMap<value, row_indices>)
    range_indexes: Arc<DashMap<String, DashMap<String, RwLock<BTreeMap<String, Vec<usize>>>>>>,
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

    pub fn import_from(map: std::collections::HashMap<String, Table>) -> Self {
        let dash = DashMap::new();
        for (k, v) in map {
            dash.insert(k, RwLock::new(v));
        }
        Self { 
            tables: Arc::new(dash),
            indexes: Arc::new(DashMap::new()),
            range_indexes: Arc::new(DashMap::new()),
        }
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
                for row in &table.rows {
                    let vals = row.iter()
                        .map(|v| format!("\"{}\"", v)) // Always quote for safety
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
}

impl StructuredStore {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(DashMap::new()),
            indexes: Arc::new(DashMap::new()),
            range_indexes: Arc::new(DashMap::new()),
        }
    }

    pub fn create_index(&self, _index_name: &str, table_name: &str, column: &str) -> Result<()> {
        if let Some(table_lock) = self.tables.get(table_name) {
            let table = table_lock.read().map_err(|_| anyhow!("Lock poison"))?;
            
            let col_idx = table.columns.iter().position(|c| c.name == column)
                .ok_or(anyhow!("Column not found"))?;
            
            // Build Hash index
            let value_map: DashMap<String, Vec<usize>> = DashMap::new();
            // Build Range index (B-Tree)
            let mut range_map: BTreeMap<String, Vec<usize>> = BTreeMap::new();

            for (row_idx, row) in table.rows.iter().enumerate() {
                let val = &row[col_idx];
                value_map.entry(val.clone()).or_insert_with(Vec::new).push(row_idx);
                range_map.entry(val.clone()).or_insert_with(Vec::new).push(row_idx);
            }
            
            // Store Hash Index
            self.indexes
                .entry(table_name.to_string())
                .or_insert_with(DashMap::new)
                .insert(column.to_string(), value_map);
            
            // Store Range Index
            self.range_indexes
                .entry(table_name.to_string())
                .or_insert_with(DashMap::new)
                .insert(column.to_string(), RwLock::new(range_map));
            
            Ok(())
        } else {
            Err(anyhow!("Table not found"))
        }
    }

    pub fn create_table(&self, name: String, columns: Vec<Column>) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(anyhow!("Table already exists"));
        }
        let table = Table {
            name: name.clone(),
            columns,
            rows: Vec::new(),
        };
        self.tables.insert(name, RwLock::new(table));
        Ok(())
    }

    pub fn insert(&self, table_name: &str, values: Vec<String>) -> Result<()> {
        if let Some(table_lock) = self.tables.get(table_name) {
            let mut table = table_lock.write().map_err(|_| anyhow!("Lock poison"))?;
            if values.len() != table.columns.len() {
                return Err(anyhow!("Column count mismatch"));
            }

            // Check Primary Key Uniqueness
            // (Scan-based for now, O(N))
            if let Some(pk_idx) = table.columns.iter().position(|c| c.is_primary_key) {
                let pk_val = &values[pk_idx];
                for row in &table.rows {
                    if &row[pk_idx] == pk_val {
                        return Err(anyhow!("Constraint violation: Duplicate primary key '{}'", pk_val));
                    }
                }
            }

            // Check Foreign Key Constraints
            for (i, col) in table.columns.iter().enumerate() {
                if let Some((ref ref_table_name, ref ref_col_name)) = col.references {
                    let val = &values[i];
                    
                    // We need to look up the other table
                    // Potential deadlock if self-referencing or cyclic?
                    // Given we hold write key on 'table', we should be careful.
                    // Ideally we should verify FK *before* acquiring write lock on current table, or use read lock first?
                    // But we are in insert, we need write lock anyway.
                    // For cyclic dep, use try_read? Or just read. 
                    // DashMap allows distinct locks. As long as ref_table != table_name, we are fine.
                    // If self-referencing (table_name == ref_table_name), we already hold the lock!
                    
                    let exists = if ref_table_name == table_name {
                         // Self-reference
                         if let Some(ref_idx) = table.columns.iter().position(|c| c.name == *ref_col_name) {
                             table.rows.iter().any(|r| r[ref_idx] == *val)
                         } else {
                             return Err(anyhow!("Referenced column '{}' not found in self", ref_col_name));
                         }
                    } else {
                        // External reference
                        if let Some(ref_lock) = self.tables.get(ref_table_name) {
                            let ref_table = ref_lock.read().map_err(|_| anyhow!("Lock poison"))?;
                             if let Some(ref_idx) = ref_table.columns.iter().position(|c| c.name == *ref_col_name) {
                                 ref_table.rows.iter().any(|r| r[ref_idx] == *val)
                             } else {
                                 return Err(anyhow!("Referenced column '{}' not found in table '{}'", ref_col_name, ref_table_name));
                             }
                        } else {
                            return Err(anyhow!("Referenced table '{}' not found", ref_table_name));
                        }
                    };

                    if !exists {
                         return Err(anyhow!("Constraint violation: FK '{}' not found in '{}.{}'", val, ref_table_name, ref_col_name));
                    }
                }
            }

            let row_idx = table.rows.len(); // Index of new row
            table.rows.push(values.clone());
            
            // Maintain indexes: if any column in this table is indexed, update it
            drop(table); // Release read lock before getting write access to indexes
            
            // 1. Maintain Hash Indexes
            if let Some(table_indexes) = self.indexes.get(table_name) {
                let table_lock = self.tables.get(table_name).unwrap();
                let table = table_lock.read().map_err(|_| anyhow!("Lock poison"))?;
                
                for col_entry in table_indexes.iter() {
                    let col_name = col_entry.key();
                    if let Some(col_idx) = table.columns.iter().position(|c| &c.name == col_name) {
                        let val = &values[col_idx];
                        col_entry.value().entry(val.clone()).or_insert_with(Vec::new).push(row_idx);
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
                        let val = &values[col_idx];
                        let mut btree = col_entry.value().write().map_err(|_| anyhow!("Lock poison"))?;
                        btree.entry(val.clone()).or_insert_with(Vec::new).push(row_idx);
                    }
                }
            }
            
            Ok(())
        } else {
            Err(anyhow!("Table not found"))
        }
    }





    fn evaluate_condition(&self, row_val: &str, target_val: &str, col_type: &DataType, op: &Operator) -> bool {
        match col_type {
            DataType::Integer => {
                let r = row_val.parse::<i64>().unwrap_or(0);
                let t = target_val.parse::<i64>().unwrap_or(0);
                match op {
                    Operator::Eq => r == t,
                    Operator::Neq => r != t,
                    Operator::Gt => r > t,
                    Operator::Lt => r < t,
                    Operator::Gte => r >= t,
                    Operator::Lte => r <= t,
                    Operator::Like => false, // LIKE not supported for integers
                    Operator::In => {
                        let parts: Vec<&str> = target_val.split(',').collect();
                        parts.iter().any(|&p| p.parse::<i64>().map(|v| v == r).unwrap_or(false))
                    }
                }
            },
            DataType::Boolean => {
                let v1 = row_val.parse::<bool>();
                let v2 = target_val.parse::<bool>();
                if let (Ok(b1), Ok(b2)) = (v1, v2) {
                    match op {
                        Operator::Eq => b1 == b2,
                        Operator::Neq => b1 != b2,
                        Operator::In => {
                            let parts: Vec<&str> = target_val.split(',').collect();
                            parts.iter().any(|&p| p.parse::<bool>().map(|v| v == b1).unwrap_or(false))
                        },
                        _ => false 
                    }
                } else {
                     false
                }
            },
            DataType::String | DataType::DateTime | DataType::Blob => {
                match op {
                    Operator::Eq => row_val == target_val,
                    Operator::Neq => row_val != target_val,
                    Operator::Gt => row_val > target_val,
                    Operator::Lt => row_val < target_val,
                    Operator::Gte => row_val >= target_val,
                    Operator::Lte => row_val <= target_val,
                    Operator::Like => {
                         // Only makes sense for String, but technically works on others as string
                        let pattern = target_val.replace('%', ".*").replace('_', ".");
                        regex::Regex::new(&format!("^{}$", pattern))
                            .map(|re| re.is_match(row_val))
                            .unwrap_or(false)
                    },

                    Operator::In => {
                        target_val.split(',').any(|s| s == row_val)
                    }
                }
            },
            DataType::Float => {
                let r = row_val.parse::<f64>().unwrap_or(0.0);
                let t = target_val.parse::<f64>().unwrap_or(0.0);
                match op {
                    Operator::Eq => (r - t).abs() < f64::EPSILON,
                    Operator::Neq => (r - t).abs() >= f64::EPSILON,
                    Operator::Gt => r > t,
                    Operator::Lt => r < t,
                    Operator::Gte => r >= t,
                    Operator::Lte => r <= t,
                    Operator::In => {
                        let parts: Vec<&str> = target_val.split(',').collect();
                        parts.iter().any(|&p| p.parse::<f64>().map(|v| (v - r).abs() < f64::EPSILON).unwrap_or(false))
                    },
                    _ => false,
                }
            }
        }
    }

    fn evaluate_filter(&self, filter: &Filter, row: &Vec<String>, columns: &Vec<Column>) -> bool {
        match filter {
            Filter::Condition(col_name, op, val) => {
                if let Some(idx) = columns.iter().position(|c| c.name == *col_name) {
                    self.evaluate_condition(&row[idx], val, &columns[idx].data_type, op)
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

    fn get_optimized_indices(&self, table_name: &str, filter: &Filter) -> Option<Vec<usize>> {
        match filter {
            Filter::Condition(col, op, val) => {
                // a. Try Equality Index (Hash)
                if matches!(op, Operator::Eq) {
                    if let Some(table_indexes) = self.indexes.get(table_name) {
                        if let Some(col_index) = table_indexes.get(col) {
                            if let Some(row_indices) = col_index.get(val) {
                                return Some(row_indices.clone());
                            } else {
                                return Some(Vec::new());
                            }
                        }
                    }
                }
                
                // b. Try Range Index (B-Tree)
                if matches!(op, Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte) {
                    if let Some(table_ranges) = self.range_indexes.get(table_name) {
                        if let Some(col_range_lock) = table_ranges.get(col) {
                            if let Ok(btree) = col_range_lock.read() {
                                let row_indices: Vec<usize> = match op {
                                    Operator::Gt => btree.range::<str, _>((Excluded(val.as_str()), Unbounded)).flat_map(|(_, v)| v).cloned().collect(),
                                    Operator::Gte => btree.range::<str, _>((Included(val.as_str()), Unbounded)).flat_map(|(_, v)| v).cloned().collect(),
                                    Operator::Lt => btree.range::<str, _>((Unbounded, Excluded(val.as_str()))).flat_map(|(_, v)| v).cloned().collect(),
                                    Operator::Lte => btree.range::<str, _>((Unbounded, Included(val.as_str()))).flat_map(|(_, v)| v).cloned().collect(),
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
                        let r_set: std::collections::HashSet<usize> = r.into_iter().collect();
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
                        let mut set: std::collections::HashSet<usize> = l.into_iter().collect();
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
        filter: Option<Filter>,
        group_by: Option<Vec<String>>,
        having: Option<Filter>,
        order_by: Option<(String, bool)>,
        limit: Option<usize>,
        offset: Option<usize>
    ) -> Result<Vec<Vec<String>>> {
        if let Some(table_lock) = self.tables.get(table_name) {
            let table = table_lock.read().map_err(|_| anyhow!("Lock poison"))?;
            
            // 1. Filter (WHERE) - Try optimized index traversal
            let mut rows: Vec<Vec<String>> = if let Some(ref f) = filter {
                if let Some(row_indices) = self.get_optimized_indices(table_name, f) {
                    // Use optimized candidates, but still apply full filter to be safe 
                    // (handles AND cases where only one side was indexed)
                    row_indices.iter()
                        .filter_map(|&idx| table.rows.get(idx))
                        .filter(|row| self.evaluate_filter(f, row, &table.columns))
                        .cloned()
                        .collect()
                } else {
                    // Fall back to full scan
                    table.rows.iter()
                        .filter(|row| self.evaluate_filter(f, row, &table.columns))
                        .cloned()
                        .collect()
                }
            } else {
                table.rows.clone()
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
                let mut buckets: std::collections::HashMap<Vec<String>, Vec<Vec<String>>> = std::collections::HashMap::new();
                
                for row in rows {
                    let key: Vec<String> = group_indices.iter().map(|&i| row[i].clone()).collect();
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
                            // Simple comparison: assume numeric aggregates
                            let val: f64 = agg_val.parse().unwrap_or(0.0);
                            match &having_filter {
                                Filter::Condition(_, op, value) => {
                                    let cmp_val: f64 = value.parse().unwrap_or(0.0);
                                    match op {
                                        Operator::Eq => (val - cmp_val).abs() < 0.0001,
                                        Operator::Neq => (val - cmp_val).abs() >= 0.0001,
                                        Operator::Gt => val > cmp_val,
                                        Operator::Gte => val >= cmp_val,
                                        Operator::Lt => val < cmp_val,
                                        Operator::Lte => val <= cmp_val,
                                        _ => true,
                                    }
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
                        let col_type = &table.columns[col_idx].data_type;
                        rows.sort_by(|a, b| {
                            let cmp = match col_type {
                                DataType::Integer => {
                                    a[col_idx].parse::<i64>().unwrap_or(0)
                                        .cmp(&b[col_idx].parse::<i64>().unwrap_or(0))
                                },
                                DataType::Float => {
                                    a[col_idx].parse::<f64>().unwrap_or(0.0)
                                        .partial_cmp(&b[col_idx].parse::<f64>().unwrap_or(0.0))
                                        .unwrap_or(std::cmp::Ordering::Equal)
                                },
                                _ => a[col_idx].cmp(&b[col_idx])
                            };
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
            
            Ok(rows)
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
                        DataType::Integer => "0",
                        DataType::Boolean => "false",
                        DataType::Float => "0.0",
                        DataType::DateTime => "1970-01-01T00:00:00Z",
                        _ => "",
                    };
                    
                    for row in &mut table.rows {
                        row.push(default_val.to_string());
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
                        for row in &mut table.rows {
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

            for row in &mut table.rows {
                let matches = if let Some(ref f) = filter {
                    self.evaluate_filter(f, row, &columns)
                } else {
                    true 
                };

                if matches {
                    row[set_idx] = set_val.clone();
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

            table.rows.retain(|row| {
                 if let Some(ref f) = filter {
                    !self.evaluate_filter(f, row, &columns)
                } else {
                    false 
                }
            });
            Ok(())
        } else {
            Err(anyhow!("Table not found"))
        }
    }

    fn compute_aggregate(&self, selector: &Selector, rows: &Vec<Vec<String>>, columns: &Vec<Column>) -> Result<String> {
        match selector {
            Selector::Count => Ok(rows.len().to_string()),
            Selector::Sum(col) | Selector::Avg(col) | Selector::Max(col) | Selector::Min(col) => {
                 let col_idx = columns.iter().position(|c| c.name == *col)
                    .ok_or(anyhow!("Aggregate column not found"))?;
                
                 let values: Vec<f64> = rows.iter()
                    .filter_map(|r| r[col_idx].parse::<f64>().ok())
                    .collect();
                
                 match selector {
                     Selector::Sum(_) => Ok(values.iter().sum::<f64>().to_string()),
                     Selector::Avg(_) => {
                         let sum: f64 = values.iter().sum();
                         let count = values.len() as f64;
                         Ok(if count == 0.0 { "0".to_string() } else { (sum / count).to_string() })
                     },
                     Selector::Max(_) => {
                         Ok(values.iter().fold(f64::MIN, |a, b| a.max(*b)).to_string())
                     },
                     Selector::Min(_) => {
                         Ok(values.iter().fold(f64::MAX, |a, b| a.min(*b)).to_string())
                     },
                     _ => unreachable!()
                 }
            },
            Selector::All => Err(anyhow!("Cannot aggregate with *")),
        }
    }
}
