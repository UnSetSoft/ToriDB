# ToriDB Node.js SDK (⛩️)

The ToriDB Node.js SDK is the official client for interacting with ToriDB. It provides a highly intuitive, fluent interface for both NoSQL and Relational workloads.

## Installation

```bash
npm install toridb
```

## Connection

Connect using a Unified Connection URI:

```javascript
const { ToriDB } = require('toridb');

// Option A: Specify DB in URI (Recommended)
const client = new ToriDB("db://admin:secret+localhost:8569/production");

// Option B: Programmatic selection (Only if not in URI)
const client2 = new ToriDB("db://admin:secret+localhost:8569");
client2.dbName("staging"); 

await client.connect();
```

---

## 1. Key-Value & Atomic Counters
Basic operations for high-speed cache or status flags.

| Method | Description |
|--------|-------------|
| `get(key)` | Retrieves a value |
| `set(key, val)` | Stores a value (auto-stringifies objects) |
| `setEx(key, val, ttl)` | Stores a value with expiration (seconds) |
| `ttl(key)` | Gets remaining time for a key |
| `del(...keys)` | Deletes one or more keys |
| `incr(key)` / `decr(key)` | Atomic increment/decrement |

---

## 2. NoSQL Data Structures

### Lists (LPUSH/RPUSH)
```javascript
const list = client.list("my_list");
await list.push("item1"); // Atomic push
await list.pop(); // Atomic pop
const items = await list.range(0, -1);
```

### Sets
```javascript
const set = client.setOf("my_set");
await set.add("a", "b", "c");
const members = await set.members();
```

### Hashes (Objects)
```javascript
const user = client.hash("user:1001");
await user.set("name", "Tori");
await user.get("name");
await user.all();
```

### Sorted Sets
```javascript
const rank = client.sortedSet("rank");
await rank.add(100, "alice");
const top = await rank.range(0, 10);
```

### JSON Documents
Direct path-based manipulation of JSON strings using the `->` operator.
```javascript
const doc = client.json("settings");
await doc.set("theme", "dark");
await doc.set("meta->notifications", true);
const theme = await doc.get("theme");
```

---

## 2.1 Vector Similarity Search
ToriDB supports high-performance similarity search for embeddings.

```javascript
// Search for 5 nearest neighbors based on a vector column
const results = await client.table("products")
    .search("embedding_column", [0.1, 0.5, 0.9], 5);
```

---

## 2.2 Transactions (ACID)
Group multiple operations into an atomic unit.

```javascript
await client.beginTransaction();
try {
    await client.set("account:A", 100);
    await client.set("account:B", 200);
    await client.commit();
} catch (e) {
    await client.rollback();
}
```

---

## 3. Relational Model (SQL Builder)

ToriDB allows you to treat data as tables with formal schemas while maintaining NoSQL speed.

### Define a Blueprint
```javascript
const products = client.model("products", new ToriDB.Blueprint({
    sku: "text primary key",
    price: "decimal",
    tags: "json"
}));
```

### Fluent Query Builder
```javascript
const items = await products.find({ price: { "<": 100 } })
    .select(["sku", "price"])
    .orderBy("price", "asc")
    .limit(5)
    .execute();
```

---

## 4. System & Administration

The SDK includes a `system` manager for administrative tasks.

### Access Control (ACL)
```javascript
await client.system.acl.createUser("dev_user", "password123", ["+get", "+hset"]);
```

### Cluster & Replication
```javascript
await client.system.cluster.info();
await client.system.replication.slaveOf("master-host", 8569);
```

### Persistence
```javascript
await client.system.save(); // Force snapshots for current DB
await client.system.rewriteAof(); // Optimize AOF file
await client.execute("USE", "archive"); // Switch to another DB dynamically
```

---

## 5. Security Best Practices
- **Environment Variables**: Always use `process.env` for connection URIs.
- **RBAC**: create specific users for your applications instead of using the `default` administrative account.
- **Validation**: ToriDB validates types in the Relational model; use it for data that requires high integrity.

---
## License
UPL-1.0
