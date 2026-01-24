## 1. ToriDB vs. Redis & MongoDB
| Feature | Redis | MongoDB | ToriDB |
| :--- | :--- | :--- | :--- |
| **Data Model** | Pure NoSQL | Pure Document | **Hybrid SQL + NoSQL** |
| **Relational Queries** | Minimal | Aggregation Pipeline | **Native SQL (Aggregates, Joins)** |
| **Schema** | Schemaless | Schemaless | **Typed Tables** + Flexible JSON |
| **Protocol** | RESP | Custom Binary | **RESP** |
| **Latency** | Sub-ms | Milliseconds | **Sub-ms (Memory-first)** |

**Key Advantage**: ToriDB provides the speed of Redis and the document flexibility of MongoDB, but with the structured power of a SQL engine. Unlike MongoDB's complex aggregation pipelines, ToriDB allows you to use familiar SQL syntax for reports and data analysis.

## 2. ToriDB vs. SQLite
| Feature | SQLite | ToriDB |
| :--- | :--- | :--- |
| **Architecture** | Serverless / File-based | **Network-first / Client-Server** |
| **JSON Support** | Via extensions/functions | **Native Path Queries** |
| **NoSQL Types** | Limited | Native Lists, Sets, ZSets |
| **Scalability** | Single-node | **Native Clustering & Sharding** |

**Key Advantage**: While SQLite is excelente for local embedding, ToriDB is built for network-distributed environments that require the flexibility of Redis but the structure of a SQL database.

## 3. ToriDB vs. Vector Databases (Pinecone / Milvus)
| Feature | Vector Only DBs | ToriDB |
| :--- | :--- | :--- |
| **Relational Data** | None / Metadata only | **Full Typed SQL Tables** |
| **Hybrid Queries** | Limited | **Filter SQL + Search Vector** |
| **Complexity** | Requires sync from main DB | **Native Single Source of Truth** |

**Key Advantage**: many applications require both relational data (users, logs, prices) and embeddings. ToriDB eliminates the need for expensive syncing between a primary DB and a Vector DB by supporting both in a single, high-performance engine.

## 4. Why choose ToriDB?
- **Unified Logic**: One protocol, one connection, three data models (SQL, NoSQL, Vector).
- **Developer Productivity**: Use SQL for reports, NoSQL for fast caching, and Vector Search for similarity retrieval.
- **Distributed by Default**: Built-in sharding and replication for growing applications.

---
[Back to Home](./README.md)
