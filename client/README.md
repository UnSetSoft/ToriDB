# ToriDB Client for Node.js

A powerful, high-performance Node.js client for **ToriDB**. Supports Key-Value operations, NoSQL data structures, and a fluent SQL-like relational API.

## 📚 Documentation

You can read the basic documentation here: [**/doc**](./doc/)

or more detailed here: [**DeepWiki**](https://deepwiki.com/UnSetSoft/ToriDB)

## Installation

```bash
npm install toridb
```

## Quick Start

```javascript
const { ToriDB } = require('toridb');

// Initialize client (use environment variables for sensitive data!)
// Format: db://user:pass+host:port
const db = new ToriDB(process.env.TORIDB_URI || "db://default:secret+127.0.0.1:8569");

async function main() {
    await db.connect();
    
    // Simple Key-Value
    await db.set("greeting", "Hello ToriDB!");
    const val = await db.get("greeting");
    console.log(val); // "Hello ToriDB!"
    
    await db.disconnect();
}

main();
```

## Features

### 🔑 Key-Value & TTL
```javascript
await db.set("user:1", { name: "Alice" });
await db.get("user:1");

await db.setEx("session:temp", "data", 3600); // 1 hour TTL
await db.ttl("session:temp");

await db.incr("page_views");
await db.decr("inventory:stock");
```

### 📦 NoSQL Data Structures

#### Lists
```javascript
const myList = db.list("tasks");
await myList.push("task1", "task2");
await myList.pop();
const items = await myList.range(0, -1);
```

#### Sets
```javascript
const mySet = db.setOf("tags");
await mySet.add("nodejs", "database");
const members = await mySet.members();
```

#### Hashes
```javascript
const myHash = db.hash("user:meta");
await myHash.set("theme", "dark");
const theme = await myHash.get("theme");
const all = await myHash.all();
```

#### JSON
```javascript
const myJson = db.json("config");
await myJson.set("$", { port: 8080, debug: true });
const port = await myJson.get("$.port");
```

### 📊 Relational-like Modeling & SQL Queries

Define blueprints and use a fluent API to perform complex queries.

```javascript
const users = db.model("users", new ToriDB.Blueprint({
    id: "int primary key",
    name: "text",
    age: "int",
    data: "json"
}));

// Create
await users.create({ id: 1, name: "Tori", age: 5, data: { breed: "Shiba" } });

// Fluent Querying
const results = await users.find({ age: { ">": 3 } })
    .select(["name", "age"])
    .orderBy("age", "desc")
    .limit(10)
    .execute();

// Direct Table Query
const logs = db.table("system_logs").find({ level: "error" }).execute();
```

### 🛡️ System & ACL Management
```javascript
// ACL (Never hardcode passwords!)
// WARNING: "@all" grants full administrative access.
await db.system.acl.createUser("admin_user", process.env.DB_PASS, ["@all"]);

// Create a restricted user instead
await db.system.acl.createUser("app_user", process.env.APP_PASS, ["+get", "+set"]);

// Cluster & Replication
await db.system.cluster.info();
await db.system.replication.slaveOf("192.168.1.10", 8569);

// Server Info
const info = await db.system.info();
```

## License
UPL-1.0
