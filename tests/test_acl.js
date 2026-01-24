const { ToriDB } = require('../client/src/sdk');

async function testACL() {
    const admin = new ToriDB("db://default:secret+127.0.0.1:8569/test_acl");
    await admin.connect();
    console.log("--- Testing ACL ---");

    try {
        // Create User (Read Only)
        await admin.system.acl.createUser("reader", "pass123", ["+@all", "-set", "-delete"]); // Currently simplistic rules
        console.log("PASS: Create User");

        // Authenticate as Reader
        const reader = new ToriDB("db://reader:pass123+127.0.0.1:8569/test_acl");
        await reader.connect();
        console.log("PASS: Auth User");

        // Check Permissions
        try {
            await reader.set("forbidden", "val");
            console.error("FAIL: Reader should not be able to SET");
        } catch (e) {
            console.log("PASS: Blocked Write");
        }

        // Check Read
        await admin.set("allowed", "read_me");
        const val = await reader.get("allowed");
        console.assert(val === "read_me", "Reader failed to read");
        console.log("PASS: Allowed Read");

        reader.disconnect();

    } catch (e) {
        console.error("FAIL:", e);
    } finally {
        admin.disconnect();
    }
}

testACL();
