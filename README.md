# ToriDB (⛩️)

![Language](https://img.shields.io/badge/Language-Rust-orange)
![License: UPL 1.0](https://img.shields.io/badge/License-UPL%201.0-blue?style=for-the-badge)
![License: UPL-CE 1.0](https://img.shields.io/badge/License-UPL%E2%80%91CE%201.0-purple?style=for-the-badge)
![Non-Commercial](https://img.shields.io/badge/Non--Commercial-Only-red?style=for-the-badge)
![Commercial Tier](https://img.shields.io/badge/Company%20Tier-$500%2Fmo-orange?style=for-the-badge)

**ToriDB** (inspired by the Japanese *torii* gates, ⛩️) is a high-performance, distributed, and multi-model database engine. Like a *torii* represents a gateway between worlds, ToriDB bridges **SQL (Relational)**, **NoSQL (Key-Value/Document)**, and **Vector Storage** models into a single unified platform.

---

## ✨ Key Features

### 🧠 Vector Similarity Search
- **Embeddings Store**: First-class support for `Vector` data types (`Array<f64>`).
- **Similarity Search**: perform K-Nearest Neighbor searches using Cosine Similarity via the `SEARCH` command.
- **Hybrid Queries**: Combine SQL filters with semantic vector search (e.g., "Find products similar to this image, where price < 50").

### 🏛️ Relational SQL Model
- **Typed Tables**: Define schemas with `int`, `string`, `float`, `bool`, `vector`, etc.
- **ACID Transactions**: Full `BEGIN`, `COMMIT`, `ROLLBACK` support for atomic multi-statement operations.
- **Advanced Querying**: Aggregates (`COUNT`, `sum`), `JOIN` support, and complex `WHERE` filters.
- **Indexing**: High-performance B-Tree and Hash indexes.

### 📄 Flexible NoSQL & JSON
- **Modern Data Types**: Native support for Lists, Hashes, Sets, and Sorted Sets (ZSET).
- **JSON Path**: Store and query deep JSON structures (e.g., `user->settings->theme`).
- **Atomic Ops**: Native `INCR`, `DECR`, and push/pop operations.

### 🔐 Security & Reliability
- **RBAC & ACLs**: Granular user permissions and bcrypt-hashed authentication.
- **Log-Structured Persistence**: AOF Redo Log + Snapshotting with CRC32 checks.
- **Replication**: Master-Replica architecture with `PSYNC` for low-latency synchronization.

---

## 🚀 Quick Start

### 1. Run the Server
```bash
# Requires Rust
cargo run --release --bin toridb
```
*Port 8569*.

### 2. Connect via SDK
ToriDB uses a **Unified Connection URI**: `db://user:pass+host:port/dbname`.

```javascript
/* npm install toridb-client */
const { ToriDB } = require('../client/src/sdk');

const db = new ToriDB("db://default:secret+127.0.0.1:8569/data");
await db.connect();

// 1. Create Vector-Ready Table
await db.execute("CREATE", "TABLE", "products", "id:int", "name:string", "embedding:vector");

// 2. Insert Vector Data
await db.execute("INSERT", "products", "1", "Neural Engine", "[0.8, 0.2, 0.5]");

// 3. Search
const results = await db.table("products").search("embedding", [0.85, 0.2, 0.5], 5);
console.log(results);
```

### 3. Configuration
Control behavior via Environment Variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_PASSWORD` | Admin password | `secret` |
| `DB_HOST` | Bind address | `127.0.0.1` |
| `DB_PORT` | Port | `8569` |
| `DB_DATA_DIR` | Persistence path | `data` |
| `DB_WORKERS` | Thread pool size | `50` |

---

## ⚡ Performance Benchmark

Measured on current hardware (5,000 iterations per operation):

| Operation | Throughput (ops/sec) | Model |
| :--- | :--- | :--- |
| **KV SET** | ~3,200 | NoSQL |
| **KV GET** | ~5,800 | NoSQL |
| **SQL INSERT** | ~2,200 | Relational |
| **SQL SELECT (PK)** | ~80 | Relational |
| **Vector SEARCH** | ~50 | Similarity |

*See [**Performance Deep Dive**](./doc/BENCHMARKS.md) for detailed analysis.*

---

## 📚 Documentation Index

Explore the full capabilities of ToriDB:

- [**🏗️ Architecture & Internals**](./doc/ARCHITECTURE.md): request lifecycle, worker pools, and persistence.
- [**🏛️ Relational SQL & Vectors**](./doc/SQL_MODEL.md): Schema definition, Joins, and Vector similarity search.
- [**📄 NoSQL & JSON Guide**](./doc/NOSQL_MODEL.md): Lists, Sets, Hashes, and native JSON pathing.
- [**🔐 Security & Clustering**](./doc/SECURITY_CLUSTER.md): ACLs, virtual slots, and replication logs.
- [**🔌 SDK Reference**](./doc/CLIENT.md): Technical guide for the Node.js official client.
- [**📡 Protocol Specification**](./doc/PROTOCOL.md): Low-level RESP implementation details.
- [**⚡ Benchmarks**](./doc/BENCHMARKS.md): Performance metrics and analysis.

---

## 🛠️ Built With
- **[Rust](https://www.rust-lang.org/)**: Performance & Safety.
- **[Tokio](https://tokio.rs/)**: Async I/O runtime.
- **[Nom](https://github.com/rust-bakery/nom)**: Zero-copy command parsing.
- **[DashMap](https://github.com/xacrimon/dashmap)**: Concurrent in-memory storage.

---
Notes: developed in collaboration with Gemini 3