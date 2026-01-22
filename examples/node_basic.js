const { DbClient } = require('../lib/sdk');

/**
 * Basic Key-Value and Connectivity Example
 */
async function run() {
    const uri = process.argv[2] || "db://default:secret+127.0.0.1:8569/data";
    const db = new DbClient(uri);

    try {
        console.log("--- Basic Connectivity Test ---");
        await db.connect();
        console.log(`Connected to database: ${db.dbName}`);

        console.log("PING ->", await db.execute("PING"));

        console.log("\n--- Key-Value Operations ---");
        console.log("SET simple_key 'Hello from Node.js' ->", await db.execute("SET", "simple_key", "Hello from Node.js"));
        console.log("GET simple_key ->", await db.execute("GET", "simple_key"));

        console.log("\n--- Atomic Operations ---");
        await db.execute("SET", "counter", 10);
        console.log("Initial counter: 10");
        console.log("INCR counter ->", await db.execute("INCR", "counter"));
        console.log("DECR counter ->", await db.execute("DECR", "counter"));

        console.log("\n--- Expiry (TTL) ---");
        console.log("SETEX temp_key 5 'Disposable' ->", await db.execute("SETEX", "temp_key", 5, "Disposable"));
        console.log("TTL temp_key ->", await db.execute("TTL", "temp_key"), "seconds");

    } catch (err) {
        console.error("Execution Error:", err.message);
    } finally {
        db.close();
    }
}

run();
