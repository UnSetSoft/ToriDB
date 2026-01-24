const { ToriDB } = require('../client/src/sdk');

async function testKV() {
    const db = new ToriDB("db://default:secret+127.0.0.1:8569/test_kv");
    await db.connect();
    console.log("--- Testing Key-Value ---");

    try {
        // SET / GET
        await db.set("key1", "hello");
        const val = await db.get("key1");
        console.assert(val === "hello", `SET/GET failed: expected hello, got ${val}`);
        console.log("PASS: SET/GET");

        // EXPIRE
        await db.setEx("key_ttl", "temp", 120);
        const ttl = await db.ttl("key_ttl");
        console.assert(ttl > 0, "TTL failed");
        console.log("PASS: SETEX/TTL");

        // INCR/DECR
        await db.set("counter", 10);
        await db.incr("counter");
        let c = await db.get("counter");
        console.assert(c == 11, `INCR failed: expected 11, got ${c}`);

        await db.decr("counter");
        c = await db.get("counter");
        console.assert(c == 10, `DECR failed: expected 10, got ${c}`);
        console.log("PASS: INCR/DECR");

    } catch (e) {
        console.error("FAIL:", e);
    } finally {
        db.disconnect();
    }
}

testKV();
