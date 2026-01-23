# ToriDB Documentation (⛩️)

Welcome to the **ToriDB** documentation. ToriDB is a high-performance, distributed database engine that bridges the gap between Relational (SQL) and Document/Key-Value (NoSQL) worlds.

## Table of Contents
1. [Architecture Overview](./ARCHITECTURE.md) - How the engine works internally.
2. [SQL Model](./SQL_MODEL.md) - Tables, Indexes, and relational queries.
3. [NoSQL & JSON](./NOSQL_MODEL.md) - KV, Lists, Sets, ZSets, and JSON paths.
4. [Security & Clustering](./SECURITY_CLUSTER.md) - RBAC, Replication, and Sharding.
5. [Client SDK](./CLIENT.md) - Official Node.js driver and examples.
6. [Competitive Comparison](./COMPARISON.md) - How it stacks up against Redis and SQLite.

## Quick Start
### 1. Start the Server
```bash
cargo run --release
```
### 2. Connect with URI
The primary way to configure and connect is via a **Unified Connection URI**:
`db://username:password+host:port/dbname`

Default port: **8569**

### 3. Use an SDK
We provide native SDKs for the following languages:
- [Node.js SDK](../client/src/sdk.js)
- [Python SDK](../sdk.py)

---
*Created with 🚀 for high-performance applications.*
