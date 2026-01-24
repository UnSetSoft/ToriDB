

#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    Like,
    In,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Filter {
    Condition(String, Operator, String), // col, op, val
    And(Box<Filter>, Box<Filter>),
    Or(Box<Filter>, Box<Filter>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Selector {
    All,
    Columns(Vec<String>), // specific columns
    Count,
    Sum(String),  // column name
    Avg(String),
    Max(String),
    Min(String),
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub on_left: String,  // table1.col
    pub on_right: String, // table2.col
}

#[derive(Debug, PartialEq, Clone)]
pub enum AlterOp {
    Add(String, String), // name, type
    Drop(String),        // name
}

#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    // Replication
    ReplicaOf { host: String, port: String }, // "NO" "ONE" turns off replica
    Psync, // Subscribe to replication stream

    // Observability
    Info,
    ClusterInfo,
    ClusterSlots,
    ClusterMeet { host: String, port: u16 },
    ClusterAddSlots { slots: Vec<u16> },
    // Flexible (KV)
    Set { key: String, value: String }, // Simplification: value is stringified JSON
    Get { key: String },
    Del { keys: Vec<String> },
    
    // Lists
    LPush { key: String, values: Vec<String> },
    RPush { key: String, values: Vec<String> },
    LPop { key: String, count: Option<usize> },
    RPop { key: String, count: Option<usize> },
    LRange { key: String, start: i64, stop: i64 },

    // Hashes
    HSet { key: String, field: String, value: String },
    HGet { key: String, field: String },
    HGetAll { key: String },

    // Sets
    SAdd { key: String, members: Vec<String> },
    SMembers { key: String },
    
    // Sorted Sets (ZSET)
    ZAdd { key: String, score: f64, member: String },
    ZRange { key: String, start: i64, stop: i64 },
    ZScore { key: String, member: String },

    // JSON
    JsonGet { key: String, path: Option<String> },
    JsonSet { key: String, path: String, value: String },
    
    // Structured (Relational)
    CreateTable { name: String, columns: Vec<(String, String, bool, Option<(String, String)>)> }, // name, type, is_pk, references
    AlterTable { table: String, op: AlterOp },
    Insert { table: String, values: Vec<String> },
    Select { 
        table: String, 
        selector: Selector,
        join: Option<Vec<JoinClause>>, // Support multiple joins potentially
        filter: Option<Filter>,
        group_by: Option<Vec<String>>,
        having: Option<Filter>,
        order_by: Option<(String, bool)>, // (col, ascending)
        limit: Option<usize>,
        offset: Option<usize>,
    },
    VectorSearch { table: String, column: String, vector: Vec<f64>, limit: usize },
    Update { table: String, filter: Option<Filter>, set: (String, String) },
    Delete { table: String, filter: Option<Filter> },
    
    // System
    Ping,
    Save,
    CreateIndex { index_name: String, table: String, column: String },
    
    // TTL
    SetEx { key: String, value: String, ttl: u64 },
    Ttl { key: String },
    
    // Auth & Atomic
    Auth { username: Option<String>, password: String },
    AclSetUser { username: String, password: String, rules: Vec<String> },
    AclGetUser { username: String },
    AclList,
    AclDelUser { username: String },

    // Client/Management
    ClientList,
    ClientKill { addr: String },
    
    Incr { key: String },
    Decr { key: String },
    RewriteAof,
    Use { db_name: String },
    
    // Transactions
    Begin,
    Commit,
    Rollback,

}

impl Command {
    pub fn get_key(&self) -> Option<&str> {
        match self {
            Command::Set { key, .. } | Command::Get { key } | Command::SetEx { key, .. } |
            Command::Ttl { key } | Command::Incr { key } | Command::Decr { key } |
            Command::LPush { key, .. } | Command::RPush { key, .. } |
            Command::LPop { key, .. } | Command::RPop { key, .. } | Command::LRange { key, .. } |
            Command::HSet { key, .. } | Command::HGet { key, .. } | Command::HGetAll { key } |
            Command::SAdd { key, .. } | Command::SMembers { key } |
            Command::ZAdd { key, .. } | Command::ZRange { key, .. } | Command::ZScore { key, .. } |
            Command::JsonGet { key, .. } | Command::JsonSet { key, .. } => Some(key),
            _ => None,
        }
    }

    pub fn is_write(&self) -> bool {
        match self {
            Command::Set { .. } | Command::CreateTable { .. } | Command::Insert { .. } |
            Command::Update { .. } | Command::Delete { .. } | Command::AclSetUser { .. } |
            Command::LPush { .. } | Command::RPush { .. } | Command::LPop { .. } | Command::RPop { .. } |
            Command::HSet { .. } | Command::SAdd { .. } | Command::JsonSet { .. } |
            Command::SetEx { .. } | Command::Incr { .. } | Command::Decr { .. } |
            Command::AlterTable { .. } | Command::CreateIndex { .. } | Command::ReplicaOf { .. } | 
            Command::AclDelUser { .. } | Command::ClientKill { .. } | Command::ZAdd { .. } |
            Command::Commit => true,
            _ => false,
        }
    }
}
