const { ToriDB } = require('../client/src/sdk');

async function testCollections() {
    const db = new ToriDB("db://default:secret+127.0.0.1:8569/test_coll");
    await db.connect();
    console.log("--- Testing Collections ---");

    try {
        // Cleanup
        await db.execute("DEL", "mylist");
        await db.execute("DEL", "myset");
        await db.execute("DEL", "myhash");

        // LIST
        await db.list("mylist").push("a", "b");
        const range = await db.list("mylist").range(0, -1);
        console.assert(JSON.stringify(range) === JSON.stringify(["b", "a"]), `List Push failed: ${range}`);
        // LPUSH reverses order: push(a), list=[a]. push(b), list=[b,a].
        const popped = await db.list("mylist").pop();
        console.assert(popped === "b", "List Pop failed");
        console.log("PASS: LIST");

        // SET
        await db.setOf("myset").add("x", "y", "x");
        const members = await db.setOf("myset").members();
        console.assert(members.length === 2, "Set Unique check failed");
        console.assert(members.includes("x"), "Set member missing");
        console.log("PASS: SET");

        // HASH
        await db.hash("myhash").set("field1", "val1");
        const hval = await db.hash("myhash").get("field1");
        console.assert(hval === "val1", "Hash Get failed");
        console.log("PASS: HASH");

    } catch (e) {
        console.error("FAIL:", e);
    } finally {
        db.disconnect();
    }
}

testCollections();
