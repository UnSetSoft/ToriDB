const { DbClient } = require('../lib/sdk');

/**
 * NoSQL (Lists, Sets, Sorted Sets) and JSON Example
 */
async function run() {
    const uri = process.argv[2] || "db://default:secret+127.0.0.1:8569/nosql_test";
    const db = new DbClient(uri);

    try {
        await db.connect();

        console.log("--- NoSQL: Lists ---");
        await db.execute("LPUSH", "tasks", "Task 1");
        await db.execute("LPUSH", "tasks", "Task 2");
        console.log("Pop from tasks:", await db.execute("LPOP", "tasks"));

        console.log("\n--- NoSQL: Sorted Sets (ZSET) ---");
        await db.execute("ZADD", "leaderboard", 100, "player1");
        await db.execute("ZADD", "leaderboard", 250, "player2");
        await db.execute("ZADD", "leaderboard", 150, "player3");
        console.log("ZSET Range (all):", await db.execute("ZRANGE", "leaderboard", 0, -1));

        console.log("\n--- JSON Operations ---");
        const profile = {
            id: 101,
            name: "John Doe",
            settings: { theme: "dark", notifications: true },
            tags: ["dev", "nodejs"]
        };

        console.log("Storing JSON profile...");
        await db.execute("JSON.SET", "user:101", "$", JSON.stringify(profile));

        console.log("Querying JSON path (settings.theme):");
        const theme = await db.execute("JSON.GET", "user:101", "$.settings.theme");
        console.log("Theme:", theme);

        console.log("Querying JSON path (tags[0]):");
        const firstTag = await db.execute("JSON.GET", "user:101", "$.tags[0]");
        console.log("First Tag:", firstTag);

    } catch (err) {
        console.error("Execution Error:", err.message);
    } finally {
        db.close();
    }
}

run();
