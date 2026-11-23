//! Parallel worker pool for backfill operations.

use std::sync::Arc;

use chrono::NaiveDate;
use futures::stream::{self, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::sync::mpsc;
use tracing::info;

use crate::client::ThetaDataClient;
use crate::config::Config;
use crate::db::Database;
use crate::downloaders::{BackfillResult, Downloader};
use crate::error::Result;
use crate::models::{AssetType, DataType};

/// Task for a worker to process
#[derive(Debug, Clone)]
pub struct WorkerTask {
    pub symbol: String,
    pub asset_type: AssetType,
    pub data_type: DataType,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
}

/// Statistics for the overall backfill run
#[derive(Debug, Clone, Default)]
pub struct BackfillStats {
    pub total_tasks: usize,
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub total_records: usize,
    pub total_duration_seconds: f64,
    pub errors: Vec<String>,
}

impl BackfillStats {
    pub fn success_rate(&self) -> f64 {
        if self.total_tasks == 0 {
            0.0
        } else {
            (self.total_tasks - self.failed_tasks) as f64 / self.total_tasks as f64 * 100.0
        }
    }
}

/// Worker pool for parallel backfill operations
pub struct WorkerPool {
    config: Arc<Config>,
    client: Arc<ThetaDataClient>,
    db: Arc<Database>,
}

impl WorkerPool {
    /// Create a new worker pool
    pub async fn new(config: Config) -> Result<Self> {
        let client = ThetaDataClient::new(&config)?;
        let db = Database::connect(&config).await?;

        Ok(Self {
            config: Arc::new(config),
            client: Arc::new(client),
            db: Arc::new(db),
        })
    }

    /// Run backfill for multiple tasks in parallel
    pub async fn run_backfill(
        &self,
        tasks: Vec<WorkerTask>,
        downloaders: Vec<Arc<dyn Downloader>>,
    ) -> Result<BackfillStats> {
        use std::time::Instant;

        let start = Instant::now();
        let total_tasks = tasks.len();
        let concurrency = self.config.effective_worker_count();

        info!("Starting backfill with {} tasks using {} workers", total_tasks, concurrency);

        // Set up progress bar
        let progress = ProgressBar::new(total_tasks as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-")
        );

        // Channel for results
        let (tx, mut rx) = mpsc::channel::<BackfillResult>(concurrency);

        // Process tasks concurrently
        let client = self.client.clone();
        let db = self.db.clone();
        let config = self.config.clone();

        // Create task stream
        let task_stream = stream::iter(tasks.into_iter().map(|task| {
            let client = client.clone();
            let db = db.clone();
            let config = config.clone();
            let downloaders = downloaders.clone();
            let tx = tx.clone();

            async move {
                // Find the right downloader for this task
                let downloader = downloaders.iter().find(|d| {
                    d.asset_type() == task.asset_type && d.data_type() == task.data_type
                });

                let result = if let Some(downloader) = downloader {
                    downloader.backfill(
                        &client, &db, &config,
                        &task.symbol, task.start_date, task.end_date,
                    ).await
                } else {
                    Ok(BackfillResult {
                        symbol: task.symbol.clone(),
                        data_type: task.data_type.clone(),
                        start_date: task.start_date,
                        end_date: task.end_date,
                        errors: vec!["No downloader found".to_string()],
                        ..Default::default()
                    })
                };

                if let Ok(res) = result {
                    let _ = tx.send(res).await;
                }
            }
        }));

        // Spawn task processor
        let handle = tokio::spawn(async move {
            task_stream.buffer_unordered(concurrency).collect::<Vec<_>>().await;
        });

        // Drop the sender so the receiver knows when we're done
        drop(tx);

        // Collect results
        let mut stats = BackfillStats {
            total_tasks,
            ..Default::default()
        };

        while let Some(result) = rx.recv().await {
            progress.inc(1);
            stats.completed_tasks += 1;
            stats.total_records += result.records_inserted;
            
            if !result.errors.is_empty() {
                stats.failed_tasks += 1;
                for error in result.errors {
                    stats.errors.push(format!("{}: {}", result.symbol, error));
                }
            }
        }

        handle.await.ok();
        progress.finish();

        stats.total_duration_seconds = start.elapsed().as_secs_f64();

        info!(
            "Backfill complete: {} tasks, {} records, {:.1}s",
            stats.completed_tasks, stats.total_records, stats.total_duration_seconds
        );

        Ok(stats)
    }

    /// Get available symbols for an asset type
    pub async fn get_symbols(&self, downloader: &dyn Downloader) -> Result<Vec<String>> {
        downloader.get_symbols(&self.client).await
    }

    /// Get date range for a symbol
    pub async fn get_date_range(
        &self,
        downloader: &dyn Downloader,
        symbol: &str,
    ) -> Result<(NaiveDate, NaiveDate)> {
        let dates = downloader.get_dates(&self.client, symbol).await?;
        if dates.is_empty() {
            let today = chrono::Local::now().date_naive();
            return Ok((today, today));
        }
        Ok((*dates.iter().min().unwrap(), *dates.iter().max().unwrap()))
    }
}
