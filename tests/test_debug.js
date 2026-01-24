const { ToriDB } = require('../client/src/sdk');

async function test() {
  const db = new ToriDB("db://default:secret+127.0.0.1:8569/debug");
  await db.connect();
  try {
    console.log("Sending SETEX...");
    // Send quoted
    const res = await db.execute("SETEX", "key", '"val"', "100");
    console.log("Result:", res);
  } catch (e) {
    console.error("Error:", e);
  }
  db.disconnect();
}
test();
