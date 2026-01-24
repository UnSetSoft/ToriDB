const { ToriDB } = require('../client/src/sdk');

async function test() {
    const client = new ToriDB("db://default:secret+localhost:8569");
    await client.connect();

    try {
        console.log("--- Setting up Tables ---");
        // 1. Users
        const usersBp = new ToriDB.Blueprint({
            id: { type: 'INT', primary: true },
            name: { type: 'STRING' },
            active: { type: 'BOOL' }
        });
        const Users = client.model("users_test", usersBp);
        
        // 2. Orders
        const ordersBp = new ToriDB.Blueprint({
            id: { type: 'INT', primary: true },
            user_id: { type: 'INT' },
            amount: { type: 'FLOAT' }
        });
        const Orders = client.model("orders_test", ordersBp);

        // Insert
        await Users.create({ id: 1, name: "Alice", active: true });
        await Users.create({ id: 2, name: "Bob", active: false });
        
        await Orders.create({ id: 101, user_id: 1, amount: 50.5 });
        await Orders.create({ id: 102, user_id: 1, amount: 200.0 });
        await Orders.create({ id: 103, user_id: 2, amount: 10.0 });

        console.log("--- Test 1: Column Projection ---");
        const projected = await Users.find({ active: true })
            .select(["name", "id"]) // Specific columns
            .execute();
        console.log("Projected (Expected: name, id for Alice):", projected);

        console.log("--- Test 2: JOIN ---");
        const joined = await Users.find({ name: "Alice" })
            .join("orders_test", "users_test.id", "orders_test.user_id")
            .select(["users_test.name", "orders_test.amount"])
            .execute();
        
        console.log("Joined Rows (Expected: Alice's orders):");
        console.log(joined);

        // Cleanup
        // await client.execute("DROP", "TABLE", "users_test");
        // await client.execute("DROP", "TABLE", "orders_test");

    } catch (e) {
        console.error("Test Failed:", e);
    } finally {
        client.disconnect();
    }
}

test();
