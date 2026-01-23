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
const client = new ToriDB("db://admin:secret+localhost:8569");

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
| `incr(key)` / `decr(key)` | Atomic increment/decrement |

---

## 2. NoSQL Data Structures

### Lists (LPUSH/RPUSH)
```javascript
const list = client.list("my_list");
await list.push("item1"); // Atomic push
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
await user.all();
```

### JSON Documents
Direct path-based manipulation of JSON strings.
```javascript
const doc = client.json("settings");
await doc.set("$.theme", "dark");
const theme = await doc.get("$.theme");
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
await client.system.save(); // Force snapshots
await client.system.rewriteAof(); // Optimize AOF file
```

---

## 5. Security Best Practices
- **Environment Variables**: Always use `process.env` for connection URIs.
- **RBAC**: create specific users for your applications instead of using the `default` administrative account.
- **Validation**: ToriDB validates types in the Relational model; use it for data that requires high integrity.

---
## License
UPL-1.0
