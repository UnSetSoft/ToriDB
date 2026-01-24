# Relational SQL & Vector Search (⛩️)

ToriDB provides a high-performance relational engine that combines classic SQL integrity with modern vector storage capabilities.

## 1. Schema Enforcement

ToriDB is **schema-first** for relational tables. You must define a table before inserting data.

### 1.1 Supported Data Types
| Type | Description | Example |
| :--- | :--- | :--- |
| `int` | 64-bit signed integer | `42` |
| `string` | UTF-8 UTF-8 string | `"Hello"` |
| `float` | 64-bit floating point | `3.14` |
| `bool` | Boolean (`true`/`false`) | `true` |
| `datetime`| ISO8601 or Timestamp | `"2024-01-01"` |
| `blob`   | Binary data (Base64) | `"SGVsbG8="` |
| `vector` | Float array embedding | `[0.1, 0.2, ...]` |
| `json`   | Native JSON Document | `'{"key": "val"}'` |

### 1.2 Table Definition (SQL)
```sql
CREATE TABLE users (
    id:int:pk,
    email:string,
    profile:json,
    features:vector
)
```
- `:pk` marks a column as Primary Key.
- `:fk(table.col)` marks a Foreign Key relationship.

---

## 2. Querying Data

### 2.1 Standard Select
```sql
SELECT name, email FROM users WHERE age >= 18 AND status = "active"
```

### 2.2 Aggregates & Grouping
ToriDB supports real-time aggregation over in-memory sets, including joined tables.
- **Selectors**: `COUNT(*)`, `SUM(col)`, `AVG(col)`, `MAX(col)`, `MIN(col)`.
- **Grouping**: `GROUP BY col1, col2`.
- **Filtering**: `HAVING count(*) > 5`.

### 2.3 Table Joins
Efficient in-memory joins using Hash-Join implementation.
```sql
SELECT orders.id, users.email 
FROM orders 
JOIN users ON orders.user_id = users.id
```

---

## 3. Vector Similarity Search

Perform K-Nearest Neighbor (KNN) searches across high-dimensional vectors.

### 3.1 Syntax
`SEARCH <table> <column> <pivot_vector> <limit>`

```sql
SEARCH products embedding [0.12, 0.45, 0.22] 5
```

### 3.2 Details
- **Similarity Metric**: Cosine Similarity.
- **Normalization**: Vectors are auto-normalized for consistent similarity scoring.
- **Performance**: Calculated parallelly across the worker pool.

---

## 4. Hybrid JSON Pathing

Query inside `json` columns using the **Arrow Operator (`->`)**.

```sql
SELECT email FROM users WHERE profile->settings->theme = "dark"
```

---
[Back to Document Index](../README.md)
