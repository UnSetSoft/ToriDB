use std::fs::{OpenOptions, File};
use std::io::{self, Write, BufReader, BufRead};
use tokio::sync::mpsc;


pub enum AofOp {
    Log(String),
    Rewrite(Vec<String>),
}

#[derive(Clone)]
pub struct AofLogger {
    sender: mpsc::Sender<AofOp>,
    path: String,
}

impl AofLogger {
    pub fn new(db_name: &str) -> io::Result<Self> {
        // User requested logs in /data. Defaulting to 'data'.
        let dir = std::env::var("DB_DATA_DIR").unwrap_or_else(|_| "data".to_string());
        std::fs::create_dir_all(&dir)?;
        
        let path = format!("{}/{}.db", dir, db_name);
        
        if let Some(parent) = std::path::Path::new(&path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let path_owned = path.clone();
        let (tx, mut rx) = mpsc::channel::<AofOp>(10000); 
        
        // Open file immediately to fail early if permission denied
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        let worker_path = path.to_string();

        // Use std::thread instead of tokio::spawn to isolate blocking I/O
        std::thread::spawn(move || {
            loop {
                // 1. Fetch Batch
                let mut batch = Vec::with_capacity(100);
                
                // Blocking wait for the first item
                if let Some(op) = rx.blocking_recv() {
                    batch.push(op);
                } else {
                    break; // Channel closed
                }

                // Try to drain more items (non-blocking)
                while batch.len() < 500 {
                    match rx.try_recv() {
                        Ok(op) => batch.push(op),
                        Err(_) => break, 
                    }
                }

                // 2. Process Batch (Blocking I/O)
                let mut needs_flush = false;
                
                for op in batch {
                    match op {
                        AofOp::Log(command) => {
                            let mut hasher = crc32fast::Hasher::new();
                            hasher.update(command.as_bytes());
                            let checksum = hasher.finalize();
                            
                            if let Err(e) = writeln!(file, "CRC32:{:x}:{}", checksum, command) {
                                crate::core::logger::error(&format!("AOF Write Error: {}", e));
                            }
                            needs_flush = true;
                        }
                        AofOp::Rewrite(commands) => {
                             if let Err(e) = Self::perform_rewrite(&worker_path, &commands) {
                                 crate::core::logger::error(&format!("AOF Rewrite Error: {}", e));
                             } else {
                                match OpenOptions::new().create(true).append(true).open(&worker_path) {
                                    Ok(f) => file = f,
                                    Err(e) => crate::core::logger::error(&format!("AOF Re-open Error: {}", e)),
                                }
                             }
                        }
                    }
                }

                // 3. Flush
                if needs_flush {
                    if let Err(e) = file.flush() {
                        crate::core::logger::error(&format!("AOF Flush Error: {}", e));
                    }
                }
            }
        });
        
        Ok(Self {
            sender: tx,
            path: path_owned,
        })
    }

    // Helper for rewrite logic (static/detached from self)
    fn perform_rewrite(path: &str, commands: &Vec<String>) -> io::Result<()> {
        let temp_path = format!("{}.rewrite", path);
        {
            let mut file = File::create(&temp_path)?;
            for cmd in commands {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(cmd.as_bytes());
                let checksum = hasher.finalize();
                writeln!(file, "CRC32:{:x}:{}", checksum, cmd)?;
            }
            file.flush()?;
        }
        // Atomic rename
        std::fs::rename(&temp_path, path)?;
        Ok(())
    }

    pub fn log(&self, command: &str) -> io::Result<()> {
        // Send to channel (async in background, but non-blocking here usually)
        // If buffer is full, this waits.
        // We act like it's sync IO Result for API compatibility, though we can't report write errors here immediately.
        let op = AofOp::Log(command.to_string());
        self.sender.try_send(op).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
    
    // Rewrite is now fire-and-forget from the caller's perspective (queued)
    pub fn rewrite(&self, commands: Vec<String>) -> io::Result<()> {
        let op = AofOp::Rewrite(commands);
        self.sender.blocking_send(op).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    pub fn load(&self) -> io::Result<Vec<String>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        
        let mut commands = Vec::new();
        let mut tx_buffer: Option<Vec<String>> = None;
        let mut in_transaction = false;

        for (i, line_res) in reader.lines().enumerate() {
            let line = line_res?;
            if line.trim().is_empty() { continue; }

            let command_str = if line.starts_with("CRC32:") {
                let parts: Vec<&str> = line.splitn(3, ':').collect();
                if parts.len() == 3 {
                    let stored_crc = u32::from_str_radix(parts[1], 16).unwrap_or(0);
                    let cmd = parts[2];

                    let mut hasher = crc32fast::Hasher::new();
                    hasher.update(cmd.as_bytes());
                    let computed_crc = hasher.finalize();

                    if stored_crc != computed_crc {
                        crate::core::logger::error(&format!("[CRASH RECOVERY] CRC mismatch at line {}. Corrupt data detected. Stopping load.", i + 1));
                        break; 
                    }
                    cmd.to_string()
                } else {
                     crate::core::logger::warn(&format!("[CRASH RECOVERY] Malformed AOF line at {}. Skipping.", i + 1));
                     continue;
                }
            } else {
                line
            };

            // Transaction Machine
            if command_str == "BEGIN" {
                if in_transaction {
                    crate::core::logger::warn(&format!("[CRASH RECOVERY] Found BEGIN inside active transaction at line {}. Discarding previous partial transaction.", i + 1));
                }
                in_transaction = true;
                tx_buffer = Some(Vec::new());
            } else if command_str == "COMMIT" {
                if in_transaction {
                    if let Some(buf) = tx_buffer.take() {
                        commands.extend(buf);
                    }
                    in_transaction = false;
                } else {
                    crate::core::logger::warn(&format!("[CRASH RECOVERY] Found COMMIT without active transaction at line {}. Ignoring.", i + 1));
                }
            } else {
                if in_transaction {
                   if let Some(ref mut buf) = tx_buffer {
                       buf.push(command_str);
                   }
                } else {
                    commands.push(command_str);
                }
            }
        }
        
        if in_transaction {
            crate::core::logger::warn("[CRASH RECOVERY] End of file reached with active transaction. Dropping incomplete transaction.");
        }

        Ok(commands)
    }
}
