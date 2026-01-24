use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnifiedValue {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    DateTime(i64), // Unix Timestamp
    Blob(String), // Base64 stored as string for now
    Array(Vec<UnifiedValue>),
    Object(BTreeMap<String, UnifiedValue>),
    Vector(Vec<f64>),
}

impl UnifiedValue {
    pub fn cosine_similarity(&self, other: &Self) -> Option<f64> {
        match (self, other) {
            (UnifiedValue::Vector(a), UnifiedValue::Vector(b)) => {
                if a.len() != b.len() || a.is_empty() { return None; }
                let dot_product: f64 = a.iter().zip(b).map(|(x, y)| x * y).sum();
                let norm_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
                let norm_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
                if norm_a == 0.0 || norm_b == 0.0 { return None; }
                Some(dot_product / (norm_a * norm_b))
            },
            _ => None
        }
    }
}

// Custom PartialOrd/Ord for total ordering (needed for BTreeMap keys)
// Order: Null < Boolean < Integer < Float < DateTime < String < Blob < Array < Object
impl PartialEq for UnifiedValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (UnifiedValue::Null, UnifiedValue::Null) => true,
            (UnifiedValue::Integer(a), UnifiedValue::Integer(b)) => a == b,
            (UnifiedValue::Float(a), UnifiedValue::Float(b)) => {
                if a.is_nan() && b.is_nan() { true } else { a == b }
            },
            (UnifiedValue::String(a), UnifiedValue::String(b)) => a == b,
            (UnifiedValue::Boolean(a), UnifiedValue::Boolean(b)) => a == b,
            (UnifiedValue::DateTime(a), UnifiedValue::DateTime(b)) => a == b,
            (UnifiedValue::Blob(a), UnifiedValue::Blob(b)) => a == b,
            (UnifiedValue::Array(a), UnifiedValue::Array(b)) => a == b,
            (UnifiedValue::Object(a), UnifiedValue::Object(b)) => a == b,
            (UnifiedValue::Vector(a), UnifiedValue::Vector(b)) => {
                if a.len() != b.len() { return false; }
                a.iter().zip(b).all(|(x, y)| (x - y).abs() < f64::EPSILON)
            },
            _ => false,
        }
    }
}

impl Eq for UnifiedValue {}

impl PartialOrd for UnifiedValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UnifiedValue {
    fn cmp(&self, other: &Self) -> Ordering {
        use UnifiedValue::*;
        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Less,
            (_, Null) => Ordering::Greater,

            (Boolean(a), Boolean(b)) => a.cmp(b),
            (Boolean(_), _) => Ordering::Less,
            (_, Boolean(_)) => Ordering::Greater,

            (Integer(a), Integer(b)) => a.cmp(b),
            (Integer(a), Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal),
            (Float(a), Integer(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
            (Float(a), Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            
            // Numbers are group 2. Integer/Float mixed comparisons handled above? 
            // Wait, standard Rust match arms are checked in order.
            // If I want Int/Float interop sorting, I need to group them.
            // But strict typing suggests separation or type coercion.
            // Let's group numbers for "natural" sorting if possible, 
            // but strict Ordering between different enum variants is easier if we just order by Type ID.
            // Strategy: Type ID Order.
            // Null(0) < Bool(1) < Number(2) < String(3) ...
            
            // Let's stick to strict type separation for CMP to ensure stability, 
            // BUT for Int vs Float, we might want interoperability?
            // "10" (int) vs "10.5" (float).
            // Let's keep it simple: Compare Discriminant first.
            
            (Integer(_), _) => Ordering::Less,
            (_, Integer(_)) => Ordering::Greater,

            (Float(_), _) => Ordering::Less,
            (_, Float(_)) => Ordering::Greater,

            (DateTime(a), DateTime(b)) => a.cmp(b),
            (DateTime(_), _) => Ordering::Less,
            (_, DateTime(_)) => Ordering::Greater,

            (String(a), String(b)) => a.cmp(b),
            (String(_), _) => Ordering::Less,
            (_, String(_)) => Ordering::Greater,

            (Blob(a), Blob(b)) => a.cmp(b),
            (Blob(_), _) => Ordering::Less,
            (_, Blob(_)) => Ordering::Greater,

            (Array(a), Array(b)) => a.cmp(b),
            (Array(_), _) => Ordering::Less,
            (_, Array(_)) => Ordering::Greater,

            (Object(a), Object(b)) => a.cmp(b),
            
            (Vector(_), _) => Ordering::Less,
            (_, Vector(_)) => Ordering::Greater, 
        }
    }
}

impl std::hash::Hash for UnifiedValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use UnifiedValue::*;
        std::mem::discriminant(self).hash(state);
        match self {
            Null => {},
            Integer(i) => i.hash(state),
            Float(f) => {
                // Hash float as bits. Canonicalize NaN.
                let bits = if f.is_nan() {
                    0x7ff8000000000000u64 // Canonical quiet NaN
                } else {
                    f.to_bits()
                };
                bits.hash(state);
            },
            String(s) => s.hash(state),
            Boolean(b) => b.hash(state),
            DateTime(t) => t.hash(state),
            Blob(b) => b.hash(state),
            Array(a) => a.hash(state),
            Object(o) => o.hash(state),
            Vector(v) => {
                for f in v {
                    let bits = if f.is_nan() { 0x7ff8000000000000u64 } else { f.to_bits() };
                    bits.hash(state);
                }
            }
        }
    }
}

impl fmt::Display for UnifiedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnifiedValue::Null => write!(f, "NULL"),
            UnifiedValue::Integer(i) => write!(f, "{}", i),
            UnifiedValue::Float(fl) => write!(f, "{}", fl),
            UnifiedValue::String(s) => write!(f, "{}", s),
            UnifiedValue::Boolean(b) => write!(f, "{}", b),
            UnifiedValue::DateTime(ts) => write!(f, "{}", ts),
            UnifiedValue::Blob(b) => write!(f, "<BLOB len={}>", b.len()),
            UnifiedValue::Array(arr) => write!(f, "{:?}", arr),
            UnifiedValue::Object(obj) => write!(f, "{:?}", obj),
            UnifiedValue::Vector(vec) => write!(f, "{:?}", vec),
        }
    }
}

// Conversion from serde_json::Value
impl From<serde_json::Value> for UnifiedValue {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => UnifiedValue::Null,
            serde_json::Value::Bool(b) => UnifiedValue::Boolean(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    UnifiedValue::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    UnifiedValue::Float(f)
                } else {
                    UnifiedValue::Null
                }
            },
            serde_json::Value::String(s) => UnifiedValue::String(s),
            serde_json::Value::Array(arr) => {
                UnifiedValue::Array(arr.into_iter().map(UnifiedValue::from).collect())
            },
            serde_json::Value::Object(obj) => {
                let map: BTreeMap<String, UnifiedValue> = obj.into_iter()
                    .map(|(k, v)| (k, UnifiedValue::from(v)))
                    .collect();
                UnifiedValue::Object(map)
            },
        }
    }
}

// Conversion into serde_json::Value (for serialization)
impl From<&UnifiedValue> for serde_json::Value {
    fn from(v: &UnifiedValue) -> Self {
        match v {
            UnifiedValue::Null => serde_json::Value::Null,
            UnifiedValue::Boolean(b) => serde_json::Value::Bool(*b),
            UnifiedValue::Integer(i) => serde_json::json!(i),
            UnifiedValue::Float(f) => serde_json::json!(f),
            UnifiedValue::String(s) => serde_json::Value::String(s.clone()),
            UnifiedValue::DateTime(ts) => serde_json::json!(ts),
            UnifiedValue::Blob(b) => serde_json::Value::String(b.clone()),
            UnifiedValue::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(serde_json::Value::from).collect())
            },
            UnifiedValue::Object(obj) => {
                let map: serde_json::Map<String, serde_json::Value> = obj.iter()
                    .map(|(k, v)| (k.clone(), serde_json::Value::from(v)))
                    .collect();
                serde_json::Value::Object(map)
            },
            UnifiedValue::Vector(v) => serde_json::json!(v),
        }
    }
}

