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
        let dir = std::env::var("DB_DATA_DIR").unwrap_or_else(|_| "data".to_string());
        std::fs::create_dir_all(&dir)?;
        let path = format!("{}/{}.db", dir, db_name);
        let path_owned = path.clone();
        let (tx, mut rx) = mpsc::channel::<AofOp>(10000); // Larger buffer
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        let worker_path = path.to_string();

        tokio::spawn(async move {
            loop {
                // 1. Fetch Batch
                let mut batch = Vec::with_capacity(100);
                
                // Blocking wait for at least one item
                if let Some(op) = rx.recv().await {
                    batch.push(op);
                } else {
                    break; // Channel closed
                }

                // Try to drain more items (up to limit) without blocking
                while batch.len() < 500 { // Max batch size
                    match rx.try_recv() {
                        Ok(op) => batch.push(op),
                        Err(_) => break, // Empty or Closed
                    }
                }

                // 2. Process Batch
                let mut needs_flush = false;
                eprintln!("DEBUG: Batch size: {}", batch.len());

                for op in batch {
                    match op {
                        AofOp::Log(command) => {
                            let mut hasher = crc32fast::Hasher::new();
                            hasher.update(command.as_bytes());
                            let checksum = hasher.finalize();
                            
                            if let Err(e) = writeln!(file, "CRC32:{:x}:{}", checksum, command) {
                                eprintln!("AOF Write Error: {}", e);
                            }
                            needs_flush = true;
                        }
                        AofOp::Rewrite(commands) => {
                             // Perform rewrite logic
                             // This is synchronous/blocking in this thread, which is fine (serialized)
                             if let Err(e) = Self::perform_rewrite(&worker_path, &commands) {
                                 eprintln!("AOF Rewrite Error: {}", e);
                             } else {
                                // Re-open file log after rewrite
                                match OpenOptions::new().create(true).append(true).open(&worker_path) {
                                    Ok(f) => file = f,
                                    Err(e) => eprintln!("AOF Re-open Error: {}", e),
                                }
                             }
                        }
                    }
                }

                // 3. Flush (Group Commit)
                if needs_flush {
                    if let Err(e) = file.flush() {
                        eprintln!("AOF Flush Error: {}", e);
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
        for (i, line_res) in reader.lines().enumerate() {
            let line = line_res?;
            if line.trim().is_empty() { continue; }

            if line.starts_with("CRC32:") {
                let parts: Vec<&str> = line.splitn(3, ':').collect();
                if parts.len() == 3 {
                    let stored_crc = u32::from_str_radix(parts[1], 16).unwrap_or(0);
                    let command = parts[2];

                    let mut hasher = crc32fast::Hasher::new();
                    hasher.update(command.as_bytes());
                    let computed_crc = hasher.finalize();

                    if stored_crc != computed_crc {
                        eprintln!("[CRASH RECOVERY] CRC mismatch at line {}. Corrupt data detected. Stopping load.", i + 1);
                        break; 
                    } else {
                        commands.push(command.to_string());
                    }
                } else {
                     eprintln!("[CRASH RECOVERY] Malformed AOF line at {}. Skipping.", i + 1);
                }
            } else {
                commands.push(line);
            }
        }
        Ok(commands)
    }
}
