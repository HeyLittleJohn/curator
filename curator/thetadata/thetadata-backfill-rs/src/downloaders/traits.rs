//! Downloader trait definition.

use async_trait::async_trait;
use chrono::NaiveDate;

use crate::client::ThetaDataClient;
use crate::config::{BackfillMode, Config};
use crate::db::Database;
use crate::error::Result;
use crate::models::{AssetType, DataType};

/// Result of a backfill operation
#[derive(Debug, Clone, Default)]
pub struct BackfillResult {
    pub symbol: String,
    pub data_type: DataType,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub records_downloaded: usize,
    pub records_inserted: usize,
    pub errors: Vec<String>,
    pub duration_seconds: f64,
}

impl BackfillResult {
    pub fn new(symbol: &str, data_type: DataType, start_date: NaiveDate, end_date: NaiveDate) -> Self {
        Self {
            symbol: symbol.to_string(),
            data_type,
            start_date,
            end_date,
            ..Default::default()
        }
    }

    pub fn is_success(&self) -> bool {
        self.errors.is_empty()
    }
}

/// Trait for all data downloaders
#[async_trait]
pub trait Downloader: Send + Sync {
    /// Get the asset type for this downloader
    fn asset_type(&self) -> AssetType;

    /// Get the data type for this downloader
    fn data_type(&self) -> DataType;

    /// Get available symbols
    async fn get_symbols(&self, client: &ThetaDataClient) -> Result<Vec<String>>;

    /// Get available dates for a symbol
    async fn get_dates(&self, client: &ThetaDataClient, symbol: &str) -> Result<Vec<NaiveDate>>;

    /// Download and insert data for a single date
    async fn download_date(
        &self,
        client: &ThetaDataClient,
        db: &Database,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<usize>;

    /// Run backfill for a symbol
    async fn backfill(
        &self,
        client: &ThetaDataClient,
        db: &Database,
        config: &Config,
        symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<BackfillResult> {
        use std::time::Instant;

        let start = Instant::now();
        let mut result = BackfillResult::new(symbol, self.data_type(), start_date, end_date);

        // Get available dates
        let all_dates = self.get_dates(client, symbol).await?;
        let mut dates_in_range: Vec<NaiveDate> = all_dates
            .into_iter()
            .filter(|d| *d >= start_date && *d <= end_date)
            .collect();

        // For incremental mode, filter out existing dates
        if config.backfill_mode == BackfillMode::Incremental {
            let table = self.table_name();
            let existing = db.get_existing_dates(&table, symbol, "timestamp").await?;
            dates_in_range.retain(|d| !existing.contains(d));
        }

        // Download each date
        for date in dates_in_range {
            match self.download_date(client, db, symbol, date).await {
                Ok(count) => {
                    result.records_downloaded += count;
                    result.records_inserted += count;
                }
                Err(e) => {
                    result.errors.push(format!("{}: {}", date, e));
                }
            }
        }

        result.duration_seconds = start.elapsed().as_secs_f64();
        Ok(result)
    }

    /// Get the database table name for this downloader
    fn table_name(&self) -> String;
}
