use dashmap::DashMap;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Instant, Duration};

// Internal entry to track access time
#[derive(Clone)]
struct Entry {
    value: Value,
    last_accessed: Instant,
}

#[derive(Clone)]
pub struct FlexibleStore {
    data: Arc<DashMap<String, Entry>>,
    expiry: Arc<DashMap<String, Instant>>,
    sorted_sets: Arc<DashMap<String, Vec<(f64, String)>>>, // ZSET: key -> [(score, member)]
    max_keys: usize,
}

impl FlexibleStore {
    pub fn new() -> Self {
        // limit from env or default
        let max = std::env::var("DB_MAX_KEYS")
            .unwrap_or("10000".to_string())
            .parse()
            .unwrap_or(10_000);
            
        Self {
            data: Arc::new(DashMap::new()),
            expiry: Arc::new(DashMap::new()),
            sorted_sets: Arc::new(DashMap::new()),
            max_keys: max,
        }
    }

    fn evict_if_needed(&self) {
        if self.data.len() >= self.max_keys {
            // Approximated LRU: Sample 5 keys, evict oldest
            // let mut rng = rand::rng();
            // DashMap iter is locking per shard, we need to be careful.
            // But we just need random keys.
            // DashMap doesn't support random sampling efficiently without iteration.
            // Iterating whole map is slow.
            // Strategy: 
            // 1. We just iterate and take first 5? No, that's not random (hash order).
            //    Hash order is effectively random enough for this? Maybe.
            // 2. Or assume we only need to evict *some* old key.
            // Let's take the first 5 entries from the iterator (pseudo-random due to hash).
            
            let victim = self.data.iter()
                .take(5)
                .min_by_key(|entry| entry.value().last_accessed);
            
            if let Some(v) = victim {
                let key = v.key().clone();
                // Drop ref before remove to avoid deadlock if any
                drop(v); 
                self.data.remove(&key);
                self.expiry.remove(&key);
            }
        }
    }

    pub fn set(&self, key: String, value: Value) {
        if !self.data.contains_key(&key) {
            self.evict_if_needed();
        }
        
        let entry = Entry {
            value,
            last_accessed: Instant::now(),
        };
        self.data.insert(key.clone(), entry);
        self.expiry.remove(&key); 
    }

    pub fn set_with_ttl(&self, key: String, value: Value, ttl_secs: u64) {
        if !self.data.contains_key(&key) {
            self.evict_if_needed();
        }

        let entry = Entry {
            value,
            last_accessed: Instant::now(),
        };
        self.data.insert(key.clone(), entry);
        self.expiry.insert(key, Instant::now() + Duration::from_secs(ttl_secs));
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        // Check if expired
        if let Some(exp) = self.expiry.get(key) {
            if Instant::now() > *exp {
                self.data.remove(key);
                self.expiry.remove(key);
                return None;
            }
        }
        
        // Update last_accessed
        if let Some(mut entry) = self.data.get_mut(key) {
             entry.last_accessed = Instant::now();
             return Some(entry.value.clone());
        }
        None
    }
    
    #[allow(dead_code)]
    pub fn delete(&self, key: &str) {
        self.data.remove(key);
        self.expiry.remove(key);
    }

    pub fn ttl(&self, key: &str) -> Option<i64> {
        if let Some(exp) = self.expiry.get(key) {
            let remaining = exp.saturating_duration_since(Instant::now());
            Some(remaining.as_secs() as i64)
        } else {
            if self.data.contains_key(key) {
                Some(-1) 
            } else {
                Some(-2) // Missing
            }
        }
    }

    pub fn incr(&self, key: &str) -> i64 {
        let mut val = 0i64;
        
        // Optimistic update pattern or lock?
        // DashMap get_mut locks the shard.
        // We need to handle entry existence and eviction if new.
        
        if !self.data.contains_key(key) {
            self.evict_if_needed();
            let entry = Entry {
                value: serde_json::Value::Number(1.into()),
                last_accessed: Instant::now(),
            };
            self.data.insert(key.to_string(), entry);
            return 1;
        }

        // Update existing
        if let Some(mut entry) = self.data.get_mut(key) {
             if let Some(n) = entry.value.as_i64() {
                 val = n;
             }
             val += 1;
             entry.value = serde_json::Value::Number(val.into());
             entry.last_accessed = Instant::now();
        }
        val
    }

    pub fn decr(&self, key: &str) -> i64 {
        let mut val = 0i64;
        
        if !self.data.contains_key(key) {
            self.evict_if_needed();
            let entry = Entry {
                value: serde_json::Value::Number((-1).into()),
                last_accessed: Instant::now(),
            };
            self.data.insert(key.to_string(), entry);
            return -1;
        }

        if let Some(mut entry) = self.data.get_mut(key) {
             if let Some(n) = entry.value.as_i64() {
                 val = n;
             }
             val -= 1;
             entry.value = serde_json::Value::Number(val.into());
             entry.last_accessed = Instant::now();
        }
        val
    }

    // LISTS
    pub fn lpush(&self, key: &str, values: Vec<String>) -> usize {
        self.evict_if_needed();
        
        // Ensure key exists as Array or create new
        if !self.data.contains_key(key) {
             let entry = Entry {
                value: Value::Array(Vec::new()),
                last_accessed: Instant::now(),
            };
            self.data.insert(key.to_string(), entry);
        }

        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            if let Some(arr) = entry.value.as_array_mut() {
                for v in values {
                    arr.insert(0, Value::String(v));
                }
                return arr.len();
            }
        }
        0
    }

    pub fn rpush(&self, key: &str, values: Vec<String>) -> usize {
        self.evict_if_needed();
        
        if !self.data.contains_key(key) {
             let entry = Entry {
                value: Value::Array(Vec::new()),
                last_accessed: Instant::now(),
            };
            self.data.insert(key.to_string(), entry);
        }

        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            if let Some(arr) = entry.value.as_array_mut() {
                for v in values {
                    arr.push(Value::String(v));
                }
                return arr.len();
            }
        }
        0
    }

    pub fn lpop(&self, key: &str, count: usize) -> Vec<String> {
        let mut res = Vec::new();
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            if let Some(arr) = entry.value.as_array_mut() {
                for _ in 0..count {
                    if !arr.is_empty() {
                        if let Value::String(s) = arr.remove(0) {
                            res.push(s);
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        res
    }

    pub fn rpop(&self, key: &str, count: usize) -> Vec<String> {
        let mut res = Vec::new();
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            if let Some(arr) = entry.value.as_array_mut() {
                for _ in 0..count {
                     if let Some(Value::String(s)) = arr.pop() {
                        res.push(s);
                     } else {
                         break;
                     }
                }
            }
        }
        res
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            if let Some(arr) = entry.value.as_array() {
                let len = arr.len() as i64;
                if len == 0 { return Vec::new(); }

                let start_idx = if start < 0 { (len + start).max(0) } else { start };
                let stop_idx = if stop < 0 { (len + stop).max(0) } else { stop };
                
                let start_idx = (start_idx as usize).min(arr.len());
                let stop_idx = (stop_idx as usize).min(arr.len().saturating_sub(1)); // inclusive stop conventional in redis

                if start_idx > stop_idx { return Vec::new(); }

                let mut res = Vec::new();
                for i in start_idx..=stop_idx {
                    if let Some(Value::String(s)) = arr.get(i) {
                        res.push(s.clone());
                    }
                }
                return res;
            }
        }
        Vec::new()
    }

    // HASHES
    pub fn hset(&self, key: &str, field: String, value: String) -> usize {
        self.evict_if_needed();
        
        if !self.data.contains_key(key) {
             let entry = Entry {
                value: Value::Object(serde_json::Map::new()),
                last_accessed: Instant::now(),
            };
            self.data.insert(key.to_string(), entry);
        }

        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            if let Some(obj) = entry.value.as_object_mut() {
                let is_new = !obj.contains_key(&field);
                obj.insert(field, Value::String(value));
                return if is_new { 1 } else { 0 };
            }
        }
        0
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<String> {
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            if let Some(obj) = entry.value.as_object() {
                if let Some(Value::String(s)) = obj.get(field) {
                    return Some(s.clone());
                }
            }
        }
        None
    }

    pub fn hgetall(&self, key: &str) -> Vec<String> {
        // Returns [field1, val1, field2, val2...]
        let mut res = Vec::new();
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            if let Some(obj) = entry.value.as_object() {
                for (k, v) in obj {
                    if let Value::String(s) = v {
                        res.push(k.clone());
                        res.push(s.clone());
                    }
                }
            }
        }
        res
    }

    // SETS
    pub fn sadd(&self, key: &str, values: Vec<String>) -> usize {
        self.evict_if_needed();
         if !self.data.contains_key(key) {
             let entry = Entry {
                value: Value::Array(Vec::new()),
                last_accessed: Instant::now(),
            };
            self.data.insert(key.to_string(), entry);
        }

        let mut added = 0;
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            if let Some(arr) = entry.value.as_array_mut() {
                for v in values {
                    // Check existence (O(N) for JSON Array)
                    // Ideally use HashSet but we are backed by JSON Value
                    let v_json = Value::String(v);
                    if !arr.contains(&v_json) {
                        arr.push(v_json);
                        added += 1;
                    }
                }
            }
        }
        added
    }

    pub fn smembers(&self, key: &str) -> Vec<String> {
        let mut res = Vec::new();
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            if let Some(arr) = entry.value.as_array() {
                for v in arr {
                    if let Value::String(s) = v {
                        res.push(s.clone());
                    }
                }
            }
        }
        res
    }

    // SORTED SETS (ZSET)
    pub fn zadd(&self, key: &str, score: f64, member: String) -> i64 {
        let mut entry = self.sorted_sets.entry(key.to_string()).or_insert_with(Vec::new);
        // Remove existing member if present
        entry.retain(|(_, m)| m != &member);
        entry.push((score, member));
        entry.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        1
    }

    pub fn zrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        if let Some(entry) = self.sorted_sets.get(key) {
            let len = entry.len() as i64;
            let s = if start < 0 { (len + start).max(0) as usize } else { start as usize };
            let e = if stop < 0 { (len + stop + 1).max(0) as usize } else { (stop + 1) as usize };
            return entry.iter().skip(s).take(e.saturating_sub(s)).map(|(_, m)| m.clone()).collect();
        }
        Vec::new()
    }

    pub fn zscore(&self, key: &str, member: &str) -> Option<f64> {
        if let Some(entry) = self.sorted_sets.get(key) {
            return entry.iter().find(|(_, m)| m == member).map(|(s, _)| *s);
        }
        None
    }

    // JSON PATH
    pub fn json_get(&self, key: &str, path: Option<&str>) -> Option<String> {
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            
            let mut current = &entry.value;
            if let Some(p) = path {
                // Simple path traversal: .key.key or key.key
                // Supports array index via number? Let's keep it simple: object keys only for now or numeric keys
                let parts: Vec<&str> = p.split('.').filter(|s| !s.is_empty()).collect();
                
                for part in parts {
                    match current {
                        Value::Object(map) => {
                            if let Some(v) = map.get(part) {
                                current = v;
                            } else {
                                return None;
                            }
                        },
                        Value::Array(arr) => {
                            if let Ok(idx) = part.parse::<usize>() {
                                if let Some(v) = arr.get(idx) {
                                    current = v;
                                } else {
                                    return None;
                                }
                            } else {
                                return None;
                            }
                        },
                        _ => return None,
                    }
                }
            }
            
            // Return as stringified JSON or raw string if it is a string?
            // Redis JSON returns stringified JSON usually.
            // If it's a string, we might want raw string? Consistency says JSON string representation.
            return Some(current.to_string());
        }
        None
    }

    pub fn json_set(&self, key: &str, path: &str, value: Value) -> usize {
        self.evict_if_needed();
        
        // Ensure root exists (default to object)
        if !self.data.contains_key(key) {
             let entry = Entry {
                // If path is root "." or empty, we replace root. 
                // But usually JSON.SET root requires object? 
                // We default to flexible Set logic which overwrites root.
                // Assuming we are updating partial.
                value: Value::Object(serde_json::Map::new()),
                last_accessed: Instant::now(),
            };
            self.data.insert(key.to_string(), entry);
        }

        if let Some(mut entry) = self.data.get_mut(key) {
            entry.last_accessed = Instant::now();
            
            let parts: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();
            if parts.is_empty() {
                // Replace root
                entry.value = value;
                return 1;
            }

            // Use JSON Pointer syntax for nested updates
            let ptr_path = format!("/{}", parts.join("/"));
            if let Some(target) = entry.value.pointer_mut(&ptr_path) {
                *target = value;
                return 1;
            }
            
            // Path doesn't exist - try to create the last segment if parent exists
            if parts.len() >= 1 {
                let parent_path = if parts.len() == 1 {
                    String::new() // Root
                } else {
                    format!("/{}", parts[..parts.len()-1].join("/"))
                };
                let last_part = parts[parts.len()-1];
                
                let parent = if parent_path.is_empty() {
                    Some(&mut entry.value)
                } else {
                    entry.value.pointer_mut(&parent_path)
                };
                
                if let Some(p) = parent {
                    if let Some(obj) = p.as_object_mut() {
                        obj.insert(last_part.to_string(), value);
                        return 1;
                    }
                }
            }
            return 0;
        }
        0
    }

    // For Snapshotting
    pub fn export(&self) -> std::collections::HashMap<String, Value> {
        self.data.iter().map(|kv| (kv.key().clone(), kv.value().value.clone())).collect()
    }

    // For AOF Rewrite
    pub fn dump_commands(&self) -> Vec<String> {
        let mut commands = Vec::new();
        for kv in self.data.iter() {
            let key = kv.key();
            let entry = kv.value();
            
            // Check expiry
            if let Some(exp) = self.expiry.get(key) {
                if Instant::now() > *exp {
                    continue; // Skip expired
                }
                let ttl = exp.duration_since(Instant::now()).as_secs();
                commands.push(format!("SETEX {} {} {}", key, ttl, entry.value));
            } else {
                commands.push(format!("SET {} {}", key, entry.value));
            }
        }
        commands
    }

    pub fn import_from(map: std::collections::HashMap<String, Value>) -> Self {
        let dash = DashMap::new();
        for (k, v) in map {
            dash.insert(k, Entry { value: v, last_accessed: Instant::now() });
        }
        // Limit
        let max = std::env::var("DB_MAX_KEYS")
            .unwrap_or("10000".to_string())
            .parse()
            .unwrap_or(10_000);
            
        Self { 
            data: Arc::new(dash),
            expiry: Arc::new(DashMap::new()),
            sorted_sets: Arc::new(DashMap::new()),
            max_keys: max,
        }
    }
}
