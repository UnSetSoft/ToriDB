const net = require('net');

async function sendCommand(client, command) {
    return new Promise((resolve, reject) => {
        // console.log("Sending:", command);
        client.write(command + '\n');
        client.once('data', (data) => {
            let str = data.toString().trim();
            // Handle Bulk String ($len\r\ncontent)
            if (str.startsWith('$')) {
                const parts = str.split('\r\n');
                if (parts.length >= 2) str = parts[1];
                else if (str.includes('\n')) str = str.split('\n')[1]; // Fallback
            }
            resolve(str);
        });
    });
}

async function runTest() {
    console.log("Starting ACID Transaction Test...");
    const client = new net.Socket();

    await new Promise((resolve, reject) => {
        client.connect(8569, '127.0.0.1', () => {
            console.log("Connected.");
            resolve();
        });
        client.on('error', (e) => {
            console.error("Connection Error:", e);
            reject(e);
        });
    });

    try {
        // Authenticate
        // Try with password
        console.log("Authenticating...");
        let res = await sendCommand(client, 'AUTH default secret');
        console.log('AUTH:', res);

        if (res !== 'OK' && res !== '+OK') {
            // Maybe default user has no password? Or different format?
            // But let's assume 'secret' is correct based on Env.
            throw new Error("Auth Failed");
        }

        // Clean slate
        res = await sendCommand(client, 'SET acid_test_1 0');
        console.log('SETUP SET:', res);
        if (res !== 'OK' && res !== '+OK') console.warn("Setup SET failed:", res);

        console.log("\n--- Test 1: Commit ---");
        res = await sendCommand(client, 'BEGIN');
        console.log('BEGIN:', res);
        if (res !== 'OK' && res !== '+OK') throw new Error("BEGIN failed");

        res = await sendCommand(client, 'SET acid_test_1 100');
        console.log('SET (buffered):', res);
        if (res !== 'QUEUED') throw new Error("Expected QUEUED, got " + res);

        res = await sendCommand(client, 'GET acid_test_1');
        console.log('GET (buffered):', res);
        if (res !== 'QUEUED') throw new Error("Expected QUEUED");

        res = await sendCommand(client, 'COMMIT');
        console.log('COMMIT:', res);

        // Verify Data
        res = await sendCommand(client, 'GET acid_test_1');
        console.log('GET Verify:', res);
        if (res !== '100') throw new Error("Commit failed to apply data");

        console.log("\n--- Test 2: Rollback ---");
        await sendCommand(client, 'BEGIN');
        await sendCommand(client, 'SET acid_test_1 999');
        await sendCommand(client, 'ROLLBACK');

        res = await sendCommand(client, 'GET acid_test_1');
        console.log('GET Verify (Should be 100):', res);
        if (res !== '100') throw new Error("Rollback failed, data was changed!");

        console.log("\n--- Test 3: Dirty Read Isolation (Pipeline style) ---");
        // Reuse client 1 for transaction, new client for dirty read

        await sendCommand(client, 'BEGIN');
        await sendCommand(client, 'SET acid_test_1 500'); // Buffered

        // Client 2
        const client2 = new net.Socket();
        await new Promise(resolve => client2.connect(8569, '127.0.0.1', resolve));
        await sendCommand(client2, 'AUTH default secret');

        const view2 = await sendCommand(client2, 'GET acid_test_1');
        console.log('Client 2 View (Should be 100):', view2);
        if (view2 !== '100') throw new Error("Isolation failure: Client 2 saw uncommitted data");

        await sendCommand(client, 'COMMIT');
        const view3 = await sendCommand(client2, 'GET acid_test_1');
        console.log('Client 2 View After Commit (Should be 500):', view3);
        client2.destroy();

        if (view3 !== '500') throw new Error("Visibility failure: Client 2 didn't see committed data");


        console.log("\n✅ PASSED: Basic ACID Properties Verified.");

    } catch (e) {
        console.error("❌ FAILED:", e.message);
        process.exit(1);
    } finally {
        client.destroy();
    }
}

runTest();
