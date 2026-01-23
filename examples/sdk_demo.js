const { ToriDB } = require('../client/sdk');

async function main() {
  const client = new ToriDB("db://default:secret+localhost:8569");

  try {
    console.log("--- Connecting to ToriDB ---");
    await client.connect();
    console.log("Connected.\n");

    // 1. KV & TTL Mode
    console.log("--- KV & TTL Mode Demo ---");
    await client.set("server_name", "Tori-Main");
    await client.setEx("temp_session", { user: "admin" }, 10); // corrected signature: (key, val, ttl)

    console.log("KV Get:", await client.get("server_name"));
    console.log("TTL remains:", await client.ttl("temp_session"));
    console.log("KV Incr:", await client.incr("visits"), "\n");

    // 2. NoSQL Managers
    console.log("--- NoSQL Managers Demo ---");

    // Lists
    await client.list("logs").rpush("event1", "event2");
    console.log("List Range:", await client.list("logs").range(0, -1));

    // Hashes
    await client.hash("user:101").set("name", "Tori");
    await client.hash("user:101").set("type", "mascot");
    console.log("Hash All:", await client.hash("user:101").all());

    // Sorted Sets
    await client.sortedSet("rank").add(100, "alice");
    await client.sortedSet("rank").add(150, "bob");
    console.log("ZRange:", await client.sortedSet("rank").range(0, -1), "\n");

    // 3. Native JSON
    console.log("--- Native JSON Demo ---");
    await client.json("config").set("$", { theme: "dark", settings: { notifications: true } });
    await client.json("config").set("$.settings.notifications", false);
    console.log("JSON Get partial:", await client.json("config").get("$.settings"), "\n");

    // 4. Relational & Models
    console.log("--- Relational & Models Demo ---");
    const userBlueprint = new ToriDB.Blueprint({
      id: { type: 'INT', primary: true },
      email: { type: 'STRING', unique: true },
      profile: 'JSON'
    });

    const User = client.model("accounts", userBlueprint);
    await User.create({ id: 1, email: "tori@db.com", profile: { lang: "es" } });

    console.log("Model findById:", await User.findById(1));
    console.log("Model search:", await User.find({ email: { $like: "tori%" } }).execute(), "\n");

    // 5. System Administration
    console.log("--- System Administration Demo ---");
    console.log("Server Info (partial):", (await client.system.info()).substring(0, 50), "...");

    // ACL (Safe to try, might fail if user already exists)
    try {
      await client.system.acl.createUser("operator", "pass123", ["READ", "GET"]);
      console.log("ACL: User 'operator' created.");
    } catch (e) { }

  } catch (err) {
    console.error("SDK Demo Error:", err.message);
  } finally {
    client.disconnect();
    console.log("\nDisconnected.");
  }
}

main();
