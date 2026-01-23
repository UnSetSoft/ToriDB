# ToriDB (⛩️)

![Language](https://img.shields.io/badge/Language-Rust-orange)
![License: UPL 1.0](https://img.shields.io/badge/License-UPL%201.0-blue?style=for-the-badge)
![License: UPL-CE 1.0](https://img.shields.io/badge/License-UPL%E2%80%91CE%201.0-purple?style=for-the-badge)
![Non-Commercial](https://img.shields.io/badge/Non--Commercial-Only-red?style=for-the-badge)
![Commercial Tier](https://img.shields.io/badge/Company%20Tier-$500%2Fmo-orange?style=for-the-badge)

**ToriDB** (inspired by the Japanese *torii* gates, ⛩️) is a high-performance, distributed, and multi-model database engine. Like a *torii* represents a gateway between worlds, ToriDB bridges the gap between **Relational (SQL)** and **Document/Key-Value (NoSQL)** data models.

---

## ✨ Key Features

### 🏛️ Relational SQL Model
- **Typed Tables**: Define schemas with `int`, `string`, `float`, `bool`, etc.
- **Advanced Querying**: Aggregates (`COUNT`, `SUM`, `AVG`), `ORDER BY`, `LIMIT`, and complex `WHERE` filters.
- **Indexing**: High-performance B-Tree and Hash indexes for instant lookups.

### 📄 Flexible NoSQL & JSON
- **Modern Data Types**: Native support for Lists, Hashes, Sets, and Sorted Sets (ZSET).
- **JSON Path Queries**: Query and manipulate JSON documents using path-based syntax (e.g., `$.user.settings`).
- **Atomic Ops**: Native `INCR`, `DECR`, and push/pop operations.

### 🔐 Security & Reliability
- **RBAC & ACLs**: Granular user permissions and bcrypt-hashed authentication.
- **Log-Structured Persistence**: Redo Log (`.db`) and Snapshots (`.snap.json`) with CRC32 integrity checks.
- **Dynamic Multi-Database**: Create and switch databases on-the-fly via URI (`/dbName`) or `USE` command.
- **Isolated Storage**: Centralized in `/data` and logically segregated per database.

### 🛸 Distributed Architecture
- **Master-Replica**: Asynchronous replication for High Availability.
- **Sharding**: Cluster management with 16,384 slots and automatic redirection (`MOVED`).
- **Worker Pool**: 50-thread concurrency model for predictable latency.

---

## 🚀 Quick Start

### 1. Requirements
- Rust (Stable)
- Node.js (for SDKs)

### 2. Run the Server
```bash
cargo run --release
```
*The server will start on default port **8569**.*

### 3. Connect via URI
ToriDB uses a **Unified Connection URI** for configuration. Authentication is **statically required**:

`db://username:password+host:port/[dbName]`

> [!IMPORTANT]
> The `+` symbol is used as a separator between the credentials and the host to avoid ambiguity with the port colon.

### 4. Configuration (Environment Variables)
You can configure the server using the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_PASSWORD` | Password for the `default` user | `secret` |
| `DB_HOST` | Host address to bind to | `127.0.0.1` |
| `DB_PORT` | Port to listen on | `8569` |
| `DB_NAME` | Default database name | `data` |
| `DB_DATA_DIR` | Directory for persistence | `data` |
| `DB_WORKERS` | Number of worker threads | `50` |

> [!CAUTION]
> **Change the default password!** Always set a strong `DB_PASSWORD` before running ToriDB in any environment.

### 5. Use an SDK
- **[Node.js SDK](./lib/sdk.js)**: `const { DbClient } = require('./lib/sdk')`

---

## 📚 Documentation

You can read the basic documentation here: [**/doc**](./doc/)

or more detailed here: [**DeepWiki**](https://deepwiki.com/UnSetSoft/ToriDB)

---

## 🛠️ Built With
- **[Rust](https://www.rust-lang.org/)**: For safety and performance.
- **[Tokio](https://tokio.rs/)**: For asynchronous networking.
- **[Nom](https://github.com/rust-bakery/nom)**: For high-speed SQL/Command parsing.
- **[DashMap](https://github.com/xacrimon/dashmap)**: For concurrent in-memory data structures.

---

Notes: developed in collaboration with Gemini 3