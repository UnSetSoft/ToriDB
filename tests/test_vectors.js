const { ToriDB } = require('../client/src/sdk');

async function test() {
    console.log("--- Connecting ---");
    const client = new ToriDB("db://default:secret+localhost:8569");
    await client.connect();

    try {
        console.log("--- Setup: Dropping items if exists ---");
        // We lack DROP TABLE command in SDK easily, assuming empty or create new
        // await client.execute("DROP", "TABLE", "items"); 

        console.log("--- 1. Create Table with VECTOR ---");
        // SDK Blueprint doesn't support VECTOR type yet (need to update SDK too), 
        // using raw SQL for now.
        await client.execute("CREATE", "TABLE", "items", "id:int:pk", "embedding:vector");

        console.log("--- 2. Insert Vectors ---");
        // [1.0, 0.0] (X axis)
        await client.execute("INSERT", "items", "1", "[1.0, 0.0]");
        // [0.707, 0.707] (45 deg)
        await client.execute("INSERT", "items", "2", "[0.707, 0.707]");
        // [0.0, 1.0] (Y axis)
        await client.execute("INSERT", "items", "3", "[0.0, 1.0]");

        console.log("--- 3. Search: Nearest to [1.0, 0.0] ---");
        // Expected: id 1 (1.0), id 2 (~0.7), id 3 (0.0)
        const res1 = await client.execute("SEARCH", "items", "embedding", "[1.0, 0.0]", "3");
        console.log("Results (Near X):", res1);

        console.log("--- 4. Search: Nearest to [0.0, 1.0] ---");
        // Expected: id 3 (1.0), id 2 (~0.7), id 1 (0.0)
        const res2 = await client.execute("SEARCH", "items", "embedding", "[0.0, 1.0]", "3");
        console.log("Results (Near Y):", res2);

    } catch (e) {
        console.error("Test Failed:", e);
    } finally {
        client.disconnect();
    }
}

test();
