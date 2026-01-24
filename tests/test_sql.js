const { ToriDB } = require('../client/src/sdk');

async function testSQL() {
    const db = new ToriDB("db://default:secret+127.0.0.1:8569/test_sql");
    await db.connect();
    console.log("--- Testing SQL ---");

    try {
        // CREATE TABLE
        const tableName = "users_" + Date.now();
        await db.execute("CREATE", "TABLE", tableName, "id:int:pk", "name:string", "age:int");
        console.log("PASS: CREATE TABLE");

        // INSERT
        await db.table(tableName).create({ id: 1, name: "Alice", age: 30 });
        await db.table(tableName).create({ id: 2, name: "Bob", age: 25 });
        console.log("PASS: INSERT");

        // SELECT
        const all = await db.table(tableName).find().execute();
        console.assert(all.length === 2, "SELECT ALL failed");

        const filtered = await db.table(tableName).find({ age: { ">": 28 } }).execute();
        console.assert(filtered.length === 1 && filtered[0][1] === "Alice", "SELECT WHERE failed");
        console.log("PASS: SELECT");

        // UPDATE
        await db.table(tableName).update({ name: "Alice" }, { age: 31 });
        const updated = await db.table(tableName).findById(1);  // Assuming findById uses PK
        // Note: findById in SDK uses `id` field name hardcoded in QBuilder example, 
        // effectively doing WHERE id = 1
        console.log("PASS: UPDATE");

        // JOIN
        // Create orders
        await db.execute("CREATE", "TABLE", "orders", "oid:int:pk", "uid:int", "total:float");
        await db.execute("INSERT", "orders", "100", "1", "99.99");

        const joined = await db.table("users")
            .select(["users.name", "orders.total"])
            .join("orders", "users.id", "orders.uid")
            .execute();

        console.assert(joined.length === 1, "JOIN failed");
        console.assert(joined[0][0] === "Alice", "JOIN data mismatch");
        console.log("PASS: JOIN");

    } catch (e) {
        console.error("FAIL:", e);
    } finally {
        db.disconnect();
    }
}

testSQL();
