# Relational SQL & Vector Search (⛩️)

ToriDB provides a high-performance relational engine that combines classic SQL integrity with modern vector storage capabilities.

## 1. Schema Enforcement

ToriDB is **schema-first** for relational tables. You must define a table before inserting data.

### 1.1 Supported Data Types
| Type | Description | Example |
|------|-------------|---------|
| `int` | 64-bit integer | `42` |
| `string` | UTF-8 String | `"Hello"` |
| `float` | 64-bit float | `3.14` |
| `bool` | Boolean | `true` |
| `vector` | Float array embedding | `[0.1, 0.2, ...]` |
| `json` | Native JSON | `'{"key": "val"}'` |

### 1.2 Defining Tables
Use `column:type[:pk][:fk(table.col)]`.

```sql
CREATE TABLE products (
    id:int:pk,
    name:string,
    price:float,
    embedding:vector,
    metadata:json
)
```

---

## 2. Vector Similarity Search

Vector search allows you to find items based on **semantic similarity** rather than exact keywords.

### 2.1 The SEARCH Command
**Syntax**: `SEARCH <table> <column> <pivot_vector> <limit>`

```sql
-- Find top 5 products similar to the target embedding
SEARCH products embedding [0.12, 0.45, 0.22] 5
```

### 2.2 Performance & Internals
- **Metric**: Uses **Cosine Similarity**.
- **Indexing**: Optimized for batch processing over memory-aligned float arrays.

---

## 3. Relational Queries

### 3.1 Advanced Filtering
Supports standard operators: `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `IN`.

```sql
SELECT name, price FROM products 
WHERE price > 100 AND name LIKE "Pro%"
```

### 3.2 Joins & Aggregates
ToriDB supports high-speed In-Memory Table Joins.

```sql
-- Join example
SELECT orders.id, users.name 
FROM orders 
JOIN users ON orders.user_id = users.id
```

**Aggregates**: `COUNT(*)`, `SUM(col)`, `AVG(col)`, `MAX(col)`, `MIN(col)`.

---

## 4. Hybrid JSON Querying

Relational tables can contain `json` columns. You can query inside them using the **Arrow Operator (`->`)**.

```sql
-- Query nested JSON inside an SQL table
SELECT name FROM users WHERE profile->address->zip = "90210"
```

---
[Back to Home](../README.md)
