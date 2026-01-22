# Architecture Overview

This document describes the internal mechanics of the **ToriDB** engine.

## 1. Hybrid Storage Engine
The database maintains all data in memory for high-performance throughput while providing strong persistence through a two-tiered model.

### Data Structures
- **Flexible Store (NoSQL)**: Uses highly concurrent hash maps (`DashMap`) and specialized structures (Skiplists for ZSets).
- **Structured Store (SQL)**: Maintains typed tables with B-Tree and Hash indexes for efficient relational lookups.

## 2. Persistence Model
The database implements a "Log-Structured" persistence approach:
- **Redo Log (.db)**: Every write operation is appended to an isolated `<dbname>.db` file in the `/data` directory.
- **Snapshots (.snap.json)**: On-demand or scheduled state serialization. Snapshots allow for faster startup by reducing the need to replay the entire log.
- **CRC32 Validation**: Every entry in the log is checksummed to detect and prevent data corruption after a crash.

## 3. Concurrency & Networking
### Worker Pool
Instead of spawning a thread per connection, the server uses a **Worker Pool** (default: 50 threads).
- Incoming commands are parsed and dispatched to the pool.
- This prevents thread starvation and provides predictable latency under high load.

### RESP Protocol
The database uses the **Redis Serialization Protocol (RESP)**. 
- **Efficiency**: Binary-safe and extremely easy to parse.
- **Interoperability**: High compatibility with existing networking tools.

## 4. Isolation
Database isolation is achieved through the **URI dbName**.
- Files are segregated in `/data/<dbname>.*`.
- State is logically separated per connection based on the initialization parameters.

---
[Back to Home](./README.md)
