const { ToriDB } = require('../client/src/sdk');

async function testJSON() {
    const db = new ToriDB("db://default:secret+127.0.0.1:8569/test_json");
    await db.connect();
    console.log("--- Testing JSON ---");

    try {
        // Set Root
        await db.json("doc1").set("$", { user: { name: "Neo", age: 30 }, active: true });
        
        // Get Root
        const root = await db.json("doc1").get("$");
        // Note: Server returns stringified JSON usually? Or UnifiedValue mapped to string
        // The SDK might need to parse. Current SDK returns raw string for JSON commands usually unless modified.
        // Let's assume SDK returns the raw response.
        console.log("Root:", root); 

        // Set Nested
        await db.json("doc1").set("user->age", 31);
        
        // Get Nested
        const age = await db.json("doc1").get("user->age");
        console.assert(age == 31, `Update Nested failed: ${age}`);
        console.log("PASS: JSON Path");

    } catch (e) {
        console.error("FAIL:", e);
    } finally {
        db.disconnect();
    }
}

testJSON();
