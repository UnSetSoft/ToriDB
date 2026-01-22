# SQL Model Reference

**ToriDB** supports a robust subset of SQL for structured data management, featuring typed columns, indexes, and complex filters.

## 1. Schema Definition
### CREATE TABLE
Define a structured table with specific data types.
```sql
CREATE TABLE users (
    id int pk,
    name string,
    age int,
    balance float,
    active bool
)
```
*Supported Types: `int`, `string`, `float`, `bool`, `datetime`, `blob`.*

### CREATE INDEX
Optimize lookups on non-primary-key columns.
```sql
CREATE INDEX idx_age ON users(age)
```

## 2. Data Manipulation
### INSERT
```sql
INSERT users 1 "John Doe" 30 1500.50 true
```

### UPDATE
```sql
UPDATE users SET balance = 2000.0 WHERE name = "John Doe"
```

### DELETE
```sql
DELETE FROM users WHERE active = false
```

## 3. Querying
The engine supports powerful `SELECT` features:
- **Aggregates**: `COUNT`, `SUM`, `AVG`, `MAX`, `MIN`.
- **Filtering**: `AND`, `OR`, `>=`, `<=`, `!=`, `LIKE`, `IN`.
- **Sorting**: `ORDER BY column [ASC|DESC]`.
- **Pagination**: `LIMIT 10 OFFSET 20`.

### Example
```sql
SELECT name, age FROM users WHERE age > 18 AND balance < 5000 ORDER BY age DESC LIMIT 5
```

---
[Back to Home](./README.md)
