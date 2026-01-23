use toridb::core::uri::ConnectionUri;

fn main() {
    let uris = vec![
        "db://127.0.0.1/testdb",
        "db://localhost/testdb?workers=10",
        "db://127.0.0.1/",
        "db://127.0.0.0/testdb/",
        "db://user:pass+host/db?a=b",
    ];

    for uri_str in uris {
        println!("--- Testing: {} ---", uri_str);
        match ConnectionUri::parse(uri_str) {
            Ok(u) => {
                println!("Host: {}", u.host);
                println!("Port: {}", u.port);
                println!("DB: {}", u.db_name.as_deref().unwrap_or("NONE"));
                println!("Query: {:?}", u.query);
            }
            Err(e) => println!("ERROR: {}", e),
        }
    }
}
