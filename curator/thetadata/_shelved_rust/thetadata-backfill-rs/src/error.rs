//! Error types for the backfill system.

use thiserror::Error;

/// Main error type for the backfill system
#[derive(Error, Debug)]
pub enum BackfillError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("API error: {status} - {message}")]
    Api { status: u16, message: String },

    #[error("Rate limited - retry after {retry_after} seconds")]
    RateLimit { retry_after: u64 },

    #[error("Task error: {0}")]
    Task(String),

    #[error("Worker error: {0}")]
    Worker(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parse error: {0}")]
    Parse(String),
}

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, BackfillError>;
