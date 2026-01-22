## 1. ToriDB vs. Redis & MongoDB
| Feature | Redis | MongoDB | ToriDB |
| :--- | :--- | :--- | :--- |
| **Data Model** | Pure NoSQL | Pure Document | **Hybrid SQL + NoSQL** |
| **Relational Queries** | Minimal | Aggregation Pipeline | **Native SQL (Aggregates, Joins)** |
| **Schema** | Schemaless | Schemaless | **Typed Tables** + Flexible JSON |
| **Protocol** | RESP | Custom Binary | **RESP** |
| **Latency** | Sub-ms | Milliseconds | **Sub-ms (Memory-first)** |

**Key Advantage**: ToriDB provides the speed of Redis and the document flexibility of MongoDB, but with the structured power of a SQL engine. Unlike MongoDB's complex aggregation pipelines, ToriDB allows you to use familiar SQL syntax for reports and data analysis.

## 2. UN-DB vs. SQLite
| Feature | SQLite | UN-DB |
| :--- | :--- | :--- |
| **Architecture** | Serverless / File-based | **Network-first / Client-Server** |
| **JSON Support** | Via extensions/functions | **Native Path Queries** |
| **NoSQL Types** | Limited | Native Lists, Sets, ZSets |
| **Scalability** | Single-node | **Native Clustering & Sharding** |

**Key Advantage**: While SQLite is excelente for local embedding, UN-DB is built for network-distributed environments that require the flexibility of Redis but the structure of a SQL database.

## 3. Why choose UN-DB?
- **Unified Logic**: One protocol, one connection, two data models.
- **Developer Productivity**: Use SQL for reports and aggregates, and NoSQL for fast caching and real-time state.
- **Distributed by Default**: Built-in sharding and replication for growing applications.

---
[Back to Home](./README.md)
