const { ToriDB } = require('../client/src/sdk');

async function testTx() {
    const db = new ToriDB("db://default:secret+127.0.0.1:8569/test_tx");
    await db.connect();
    console.log("--- Testing Transactions ---");

    try {
        await db.set("bal:A", 100);
        await db.set("bal:B", 0);

        // COMMIT Scenario
        await db.beginTransaction();
        await db.decr("bal:A");
        await db.incr("bal:B");
        await db.commit();

        const a = await db.get("bal:A");
        const b = await db.get("bal:B");
        console.assert(a == 99 && b == 1, `Commit failed: A=${a}, B=${b}`);
        console.log("PASS: COMMIT");

        // ROLLBACK Scenario
        await db.beginTransaction();
        await db.set("bal:A", 0); // Drain account
        await db.rollback(); // Undo

        const a2 = await db.get("bal:A");
        console.assert(a2 == 99, `Rollback failed: A=${a2}`);
        console.log("PASS: ROLLBACK");

    } catch (e) {
        console.error("FAIL:", e);
    } finally {
        db.disconnect();
    }
}

testTx();
