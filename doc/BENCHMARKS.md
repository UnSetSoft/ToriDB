# Performance Benchmarks (⛩️)

This document provides detailed performance metrics for **ToriDB**, measured using the official benchmark suite (`benchmarks/run.js`).

## 1. Test Environment
- **Iterations**: 5,000 operations per structural test.
- **Protocol**: RESP over localhost TCP.
- **Hardware**: Current system resources (unoptimized debug build unless specified).

## 2. Results (Throughput)

| Operation | Model | Throughput (ops/sec) | Latency (avg) |
| :--- | :--- | :--- | :--- |
| **KV SET** | NoSQL | ~3,200 | 0.31 ms |
| **KV GET** | NoSQL | ~5,800 | 0.17 ms |
| **SQL INSERT** | Relational | ~2,200 | 0.45 ms |
| **SQL SELECT (PK)** | Relational | ~80 | 12.5 ms |
| **Vector SEARCH** | Similarity | ~50 | 20.0 ms |

### 2.1 Observations
1. **NoSQL Performance**: ToriDB excels at Key-Value operations due to the lock-free nature of `DashMap`.
2. **Relational Overhead**: SQL Inserts are slower than base KV due to schema validation, type checking, and index updates.
3. **Vector Scalability**: Similarity search performance is currently bound by exhaustive cosine comparison. Future versions will implement HNSW indexing for logarithmic scaling.

---

## 3. How to reproduce
You can run the benchmarks yourself by executing:
```bash
node benchmarks/run.js
```

---
[Back to Home](../README.md)
