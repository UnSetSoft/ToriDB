
import { ToriDB } from './src/sdk.js';

async function run() {
    const db = new ToriDB("db://default:password@127.0.0.1:8569/debug_repro");

    // Simulate parallel requests which might trigger the race condition
    const p1 = db.set("test_key", "should_be_in_debug").catch(e => console.error("P1 error:", e));
    const p2 = db.get("test_key").catch(e => console.error("P2 error:", e));

    await Promise.all([p1, p2]);

    console.log("Written key to debug_repro");

    // Now check 'data' database to see if it leaked there
    const dbData = new ToriDB("db://default:password@127.0.0.1:8569/data");
    // Ensure we are explicitly in data (default)
    
    // We need to wait a bit or ensure the previous ops finished? awaited above.

    try {
        const val = await dbData.get("test_key");
        console.log("Value in 'data' db:", val);
        if (val === "should_be_in_debug") {
            console.error("BUG REPRODUCED: Key written to 'data' instead of 'debug_repro'");
        } else {
            console.log("Clean: Key not found in 'data'");
        }
    } catch (e) {
        console.error("Check error:", e);
    }
    
    // Cleanup
    db.disconnect();
    dbData.disconnect();
}

run();
