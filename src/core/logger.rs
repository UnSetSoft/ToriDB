use std::time::{SystemTime, UNIX_EPOCH};

pub enum LogLevel {
    Info,
    Warn,
    Error,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARNING"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

pub fn log(level: LogLevel, message: &str) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    
    // Convert to ISO 8601-like format (HH:MM:SS) for simplicity without extra dependencies
    // A proper implementation would use 'chrono', but we keep it simple for now.
    let hours = (now / 3600) % 24;
    let minutes = (now / 60) % 60;
    let seconds = now % 60;

    println!(
        "[{:02}:{:02}:{:02}] [{}] {}",
        hours, minutes, seconds, level, message
    );
}

pub fn info(message: &str) {
    log(LogLevel::Info, message);
}

pub fn warn(message: &str) {
    log(LogLevel::Warn, message);
}

pub fn error(message: &str) {
    log(LogLevel::Error, message);
}
