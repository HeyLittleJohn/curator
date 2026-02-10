//! CLI binary for ThetaData backfill.

use std::sync::Arc;

use chrono::NaiveDate;
use clap::{Parser, Subcommand, ValueEnum};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use thetadata_backfill::{
    config::{BackfillMode, Config},
    downloaders::*,
    models::{AssetType, DataType},
    worker::{WorkerPool, WorkerTask},
};

#[derive(Parser)]
#[command(name = "thetadata-backfill")]
#[command(about = "High-performance ThetaData historical data backfill")]
#[command(version)]
struct Cli {
    /// Database URL (overrides THETADATA_DATABASE_URL)
    #[arg(long, short = 'd', env = "THETADATA_DATABASE_URL")]
    database_url: String,

    /// API base URL
    #[arg(long, default_value = "http://localhost:25503/v3", env = "THETADATA_API_BASE_URL")]
    api_url: String,

    /// Number of worker tasks
    #[arg(long, short = 'w', env = "THETADATA_WORKER_COUNT")]
    workers: Option<usize>,

    /// Backfill mode
    #[arg(long, short = 'm', default_value = "full")]
    mode: BackfillModeArg,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, ValueEnum)]
enum BackfillModeArg {
    Full,
    Incremental,
}

impl From<BackfillModeArg> for BackfillMode {
    fn from(arg: BackfillModeArg) -> Self {
        match arg {
            BackfillModeArg::Full => BackfillMode::Full,
            BackfillModeArg::Incremental => BackfillMode::Incremental,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Backfill equity (stock) data
    Equities {
        /// Comma-separated list of symbols (default: all)
        #[arg(long, short = 's')]
        symbols: Option<String>,

        /// Start date (YYYY-MM-DD)
        #[arg(long)]
        start_date: Option<String>,

        /// End date (YYYY-MM-DD)
        #[arg(long)]
        end_date: Option<String>,

        /// Data types to backfill
        #[arg(long, default_value = "ohlc,eod")]
        data_types: String,
    },

    /// Backfill options data with Greeks
    Options {
        /// Comma-separated list of symbols (default: all)
        #[arg(long, short = 's')]
        symbols: Option<String>,

        /// Start date (YYYY-MM-DD)
        #[arg(long)]
        start_date: Option<String>,

        /// End date (YYYY-MM-DD)
        #[arg(long)]
        end_date: Option<String>,

        /// Data types to backfill
        #[arg(long, default_value = "trade_greeks,open_interest")]
        data_types: String,
    },

    /// Backfill index data
    Indices {
        /// Comma-separated list of symbols (default: all)
        #[arg(long, short = 's')]
        symbols: Option<String>,

        /// Start date (YYYY-MM-DD)
        #[arg(long)]
        start_date: Option<String>,

        /// End date (YYYY-MM-DD)
        #[arg(long)]
        end_date: Option<String>,

        /// Data types to backfill
        #[arg(long, default_value = "ohlc,eod")]
        data_types: String,
    },

    /// Show database status
    Status,
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

fn parse_symbols(s: &str) -> Vec<String> {
    s.split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect()
}

fn parse_data_types(s: &str) -> Vec<DataType> {
    s.split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    // Build config
    let config = Config::new(
        cli.database_url.clone(),
        Some(cli.api_url.clone()),
        cli.workers,
        None,
        Some(match cli.mode {
            BackfillModeArg::Full => "full",
            BackfillModeArg::Incremental => "incremental",
        }.to_string()),
    )?;

    // Create worker pool
    let pool = WorkerPool::new(config.clone()).await?;

    match cli.command {
        Commands::Equities { symbols, start_date, end_date, data_types } => {
            let data_types = parse_data_types(&data_types);
            run_backfill(
                &pool,
                &config,
                AssetType::Equity,
                symbols.as_deref(),
                start_date.as_deref(),
                end_date.as_deref(),
                data_types,
            ).await?;
        }

        Commands::Options { symbols, start_date, end_date, data_types } => {
            let data_types = parse_data_types(&data_types);
            run_backfill(
                &pool,
                &config,
                AssetType::Option,
                symbols.as_deref(),
                start_date.as_deref(),
                end_date.as_deref(),
                data_types,
            ).await?;
        }

        Commands::Indices { symbols, start_date, end_date, data_types } => {
            let data_types = parse_data_types(&data_types);
            run_backfill(
                &pool,
                &config,
                AssetType::Index,
                symbols.as_deref(),
                start_date.as_deref(),
                end_date.as_deref(),
                data_types,
            ).await?;
        }

        Commands::Status => {
            println!("Database status: Connected");
            // TODO: Add table counts
        }
    }

    Ok(())
}

async fn run_backfill(
    pool: &WorkerPool,
    config: &Config,
    asset_type: AssetType,
    symbols: Option<&str>,
    start_date: Option<&str>,
    end_date: Option<&str>,
    data_types: Vec<DataType>,
) -> anyhow::Result<()> {
    // Create downloaders
    let downloaders: Vec<Arc<dyn Downloader>> = match asset_type {
        AssetType::Equity => vec![
            Arc::new(EquityOhlcDownloader) as Arc<dyn Downloader>,
            Arc::new(EquityTradeDownloader),
            Arc::new(EquityEodDownloader),
        ],
        AssetType::Option => vec![
            Arc::new(OptionTradeGreeksDownloader) as Arc<dyn Downloader>,
            Arc::new(OptionGreeksDownloader),
            Arc::new(OptionOpenInterestDownloader),
        ],
        AssetType::Index => vec![
            Arc::new(IndexOhlcDownloader) as Arc<dyn Downloader>,
            Arc::new(IndexEodDownloader),
        ],
    };

    // Get symbols
    let symbol_list = if let Some(s) = symbols {
        parse_symbols(s)
    } else {
        info!("Discovering symbols...");
        pool.get_symbols(downloaders[0].as_ref()).await?
    };

    info!("Found {} symbols", symbol_list.len());

    // Get date range
    let start = start_date.and_then(parse_date);
    let end = end_date.and_then(parse_date).unwrap_or_else(|| chrono::Local::now().date_naive());

    let start = if start.is_none() && !symbol_list.is_empty() {
        let (earliest, _) = pool.get_date_range(downloaders[0].as_ref(), &symbol_list[0]).await?;
        earliest
    } else {
        start.unwrap_or(end)
    };

    info!("Date range: {} to {}", start, end);

    // Create tasks
    let mut tasks = Vec::new();
    for symbol in &symbol_list {
        for dt in &data_types {
            tasks.push(WorkerTask {
                symbol: symbol.clone(),
                asset_type,
                data_type: dt.clone(),
                start_date: start,
                end_date: end,
            });
        }
    }

    // Run backfill
    let stats = pool.run_backfill(tasks, downloaders).await?;

    // Print summary
    println!("\nBackfill Complete!");
    println!("  Total tasks:    {}", stats.total_tasks);
    println!("  Completed:      {}", stats.completed_tasks);
    println!("  Failed:         {}", stats.failed_tasks);
    println!("  Success rate:   {:.1}%", stats.success_rate());
    println!("  Records:        {}", stats.total_records);
    println!("  Duration:       {:.1}s", stats.total_duration_seconds);

    if !stats.errors.is_empty() {
        println!("\nErrors ({}):", stats.errors.len());
        for error in stats.errors.iter().take(10) {
            println!("  • {}", error);
        }
        if stats.errors.len() > 10 {
            println!("  ... and {} more", stats.errors.len() - 10);
        }
    }

    Ok(())
}
