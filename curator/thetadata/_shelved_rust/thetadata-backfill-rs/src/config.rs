//! Configuration management for the backfill system.

use serde::Deserialize;
use std::env;

use crate::error::{BackfillError, Result};

/// Backfill mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackfillMode {
    #[default]
    Full,
    Incremental,
}

impl std::str::FromStr for BackfillMode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "full" => Ok(BackfillMode::Full),
            "incremental" => Ok(BackfillMode::Incremental),
            _ => Err(format!("Invalid backfill mode: {}", s)),
        }
    }
}

impl std::fmt::Display for BackfillMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackfillMode::Full => write!(f, "full"),
            BackfillMode::Incremental => write!(f, "incremental"),
        }
    }
}

/// Application configuration
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// ThetaData API base URL
    #[serde(default = "default_api_url")]
    pub api_base_url: String,

    /// API request timeout in seconds
    #[serde(default = "default_api_timeout")]
    pub api_timeout: u64,

    /// Maximum API retries
    #[serde(default = "default_api_retries")]
    pub api_max_retries: u32,

    /// Maximum concurrent API requests
    #[serde(default = "default_api_rate_limit")]
    pub api_rate_limit: usize,

    /// PostgreSQL database URL
    pub database_url: String,

    /// Minimum database pool size
    #[serde(default = "default_pool_min")]
    pub db_pool_min_size: u32,

    /// Maximum database pool size
    #[serde(default = "default_pool_max")]
    pub db_pool_max_size: u32,

    /// Number of worker tasks (defaults to CPU count)
    pub worker_count: Option<usize>,

    /// Batch size for database inserts
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Backfill mode
    #[serde(default)]
    pub backfill_mode: BackfillMode,
}

fn default_api_url() -> String {
    "http://localhost:25503/v3".to_string()
}

fn default_api_timeout() -> u64 {
    30
}

fn default_api_retries() -> u32 {
    3
}

fn default_api_rate_limit() -> usize {
    50
}

fn default_pool_min() -> u32 {
    5
}

fn default_pool_max() -> u32 {
    20
}

fn default_batch_size() -> usize {
    1000
}

impl Config {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        // Load .env file if present
        let _ = dotenvy::dotenv();

        let database_url = env::var("THETADATA_DATABASE_URL")
            .map_err(|_| BackfillError::Config(
                "THETADATA_DATABASE_URL not set".to_string()
            ))?;

        Ok(Config {
            api_base_url: env::var("THETADATA_API_BASE_URL")
                .unwrap_or_else(|_| default_api_url()),
            api_timeout: env::var("THETADATA_API_TIMEOUT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_api_timeout),
            api_max_retries: env::var("THETADATA_API_MAX_RETRIES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_api_retries),
            api_rate_limit: env::var("THETADATA_API_RATE_LIMIT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_api_rate_limit),
            database_url,
            db_pool_min_size: env::var("THETADATA_DB_POOL_MIN_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_pool_min),
            db_pool_max_size: env::var("THETADATA_DB_POOL_MAX_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_pool_max),
            worker_count: env::var("THETADATA_WORKER_COUNT")
                .ok()
                .and_then(|s| s.parse().ok()),
            batch_size: env::var("THETADATA_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_batch_size),
            backfill_mode: env::var("THETADATA_BACKFILL_MODE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_default(),
        })
    }

    /// Get effective worker count (defaults to CPU count)
    pub fn effective_worker_count(&self) -> usize {
        self.worker_count.unwrap_or_else(num_cpus::get)
    }

    /// Create config from explicit values (for Python bindings)
    pub fn new(
        database_url: String,
        api_base_url: Option<String>,
        worker_count: Option<usize>,
        batch_size: Option<usize>,
        mode: Option<String>,
    ) -> Result<Self> {
        Ok(Config {
            api_base_url: api_base_url.unwrap_or_else(default_api_url),
            api_timeout: default_api_timeout(),
            api_max_retries: default_api_retries(),
            api_rate_limit: default_api_rate_limit(),
            database_url,
            db_pool_min_size: default_pool_min(),
            db_pool_max_size: default_pool_max(),
            worker_count,
            batch_size: batch_size.unwrap_or_else(default_batch_size),
            backfill_mode: mode
                .and_then(|m| m.parse().ok())
                .unwrap_or_default(),
        })
    }
}
