# ToriDB Node.js SDK

A powerful, type-safe client for ToriDB.

## Features
- **Unified Client**: Handles Key-Value, NoSQL, SQL, and Vector operations.
- **Connection Pooling**: Automatically manages socket connections.
- **Query Builder**: Fluent API for SQL-like queries.
- **Transaction Support**: ACID-compliant transaction management.

## Installation
```bash
npm install toridb-client
```

## detailed Usage

### Connection
```javascript
const { ToriDB } = require('toridb-client');
const db = new ToriDB("db://default:secret+127.0.0.1:8569/my_db");
await db.connect();
```

### Vector Search (AI)
```javascript
// Search for 5 nearest neighbors
const results = await db.table("items")
    .search("embedding_col", [0.1, 0.2, 0.9], 5);
```

### Transactions
```javascript
await db.beginTransaction();
try {
    await db.set("account:A", 50);
    await db.set("account:B", 150);
    await db.commit();
} catch (e) {
    await db.rollback();
}
```

### SQL & Relations
```javascript
// Query Builder
const users = await db.table("users")
    .select(["id", "name"])
    .where({ "age": { ">": 18 } })
    .orderBy("created_at", "DESC")
    .limit(10)
    .execute();
```

### Key-Value & NoSQL
```javascript
await db.set("key", "value");
await db.list("mylist").push("item1", "item2");
await db.json("config").set("theme", "dark"); // JSON Path support
```
