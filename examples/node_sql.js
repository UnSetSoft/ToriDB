const { DbClient } = require('../lib/sdk');

/**
 * SQL-like Structured Data Example
 */
async function run() {
    const uri = process.argv[2] || "db://default:secret+127.0.0.1:8569/production";
    const db = new DbClient(uri);

    try {
        await db.connect();
        console.log("--- SQL-like Operations ---");

        // 1. Create Table
        console.log("Creating table 'users'...");
        await db.execute("CREATE", "TABLE", "users", "id:int:pk", "name:string", "age:int", "active:bool");

        // 2. Create Index
        console.log("Creating index on 'age'...");
        await db.execute("CREATE", "INDEX", "idx_age", "ON", "users(age)");

        // 3. Insert Data
        console.log("Inserting records...");
        await db.execute("INSERT", "users", 1, "Alice", 30, "true");
        await db.execute("INSERT", "users", 2, "Bob", 25, "true");
        await db.execute("INSERT", "users", 3, "Charlie", 35, "false");

        // 4. Query Data
        console.log("\nQuery: SELECT * FROM users WHERE age > 28");
        const results = await db.execute("SELECT", "*", "FROM", "users", "WHERE", "age", ">", 28);
        console.log("Results:", results);

        console.log("\nQuery: SELECT COUNT FROM users");
        const count = await db.execute("SELECT", "COUNT", "FROM", "users");
        console.log("Total Users:", count);

        // 5. Update & Delete
        console.log("\nUpdating Bob's age...");
        await db.execute("UPDATE", "users", "SET", "age", "=", 26, "WHERE", "name", "=", "Bob");

        console.log("Deleting inactive users...");
        await db.execute("DELETE", "FROM", "users", "WHERE", "active", "=", "false");

        // 6. Persistence
        console.log("\nSaving database state to disk...");
        await db.execute("SAVE");

    } catch (err) {
        console.error("Execution Error:", err.message);
    } finally {
        db.close();
    }
}

run();
