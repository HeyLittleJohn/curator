//! Database layer using sqlx with PostgreSQL.

use std::collections::HashSet;

use chrono::NaiveDate;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use tracing::info;

use crate::config::Config;
use crate::error::Result;
use crate::models::*;

/// Database connection pool manager
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Create a new database connection pool
    pub async fn connect(config: &Config) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .min_connections(config.db_pool_min_size)
            .max_connections(config.db_pool_max_size)
            .connect(&config.database_url)
            .await?;

        info!("Connected to database");
        Ok(Self { pool })
    }

    /// Get a reference to the pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get existing dates for a symbol in a table
    pub async fn get_existing_dates(
        &self,
        table: &str,
        symbol: &str,
        date_column: &str,
    ) -> Result<HashSet<NaiveDate>> {
        let query = format!(
            "SELECT DISTINCT {}::date FROM {} WHERE symbol = $1",
            date_column, table
        );
        
        let rows = sqlx::query(&query)
            .bind(symbol)
            .fetch_all(&self.pool)
            .await?;

        let dates: HashSet<NaiveDate> = rows
            .into_iter()
            .filter_map(|row| row.try_get::<NaiveDate, _>(0).ok())
            .collect();

        Ok(dates)
    }

    // Equity insert methods

    /// Bulk insert equity OHLC records
    pub async fn insert_equity_ohlc(&self, records: &[EquityOhlc]) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut count = 0;
        for record in records {
            sqlx::query(r#"
                INSERT INTO equity_ohlc (symbol, timestamp, open, high, low, close, volume, count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    count = EXCLUDED.count
            "#)
            .bind(&record.symbol)
            .bind(record.timestamp)
            .bind(record.open)
            .bind(record.high)
            .bind(record.low)
            .bind(record.close)
            .bind(record.volume)
            .bind(record.count)
            .execute(&self.pool)
            .await?;
            count += 1;
        }

        Ok(count)
    }

    /// Bulk insert equity trade records
    pub async fn insert_equity_trades(&self, records: &[EquityTrade]) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut count = 0;
        for record in records {
            sqlx::query(r#"
                INSERT INTO equity_trade (symbol, timestamp, sequence, size, price, condition, exchange,
                    ext_condition1, ext_condition2, ext_condition3, ext_condition4)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (symbol, sequence) DO NOTHING
            "#)
            .bind(&record.symbol)
            .bind(record.timestamp)
            .bind(record.sequence)
            .bind(record.size)
            .bind(record.price)
            .bind(record.condition)
            .bind(record.exchange)
            .bind(record.ext_condition1)
            .bind(record.ext_condition2)
            .bind(record.ext_condition3)
            .bind(record.ext_condition4)
            .execute(&self.pool)
            .await?;
            count += 1;
        }

        Ok(count)
    }

    /// Bulk insert equity EOD records
    pub async fn insert_equity_eod(&self, records: &[EquityEod]) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut count = 0;
        for record in records {
            sqlx::query(r#"
                INSERT INTO equity_eod (symbol, trade_date, report_created, last_trade, open, high, low, close,
                    volume, count, bid, bid_size, ask, ask_size)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (symbol, trade_date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    count = EXCLUDED.count
            "#)
            .bind(&record.symbol)
            .bind(record.trade_date)
            .bind(record.report_created)
            .bind(record.last_trade)
            .bind(record.open)
            .bind(record.high)
            .bind(record.low)
            .bind(record.close)
            .bind(record.volume)
            .bind(record.count)
            .bind(record.bid)
            .bind(record.bid_size)
            .bind(record.ask)
            .bind(record.ask_size)
            .execute(&self.pool)
            .await?;
            count += 1;
        }

        Ok(count)
    }

    // Option insert methods

    /// Bulk insert option trade greeks records
    pub async fn insert_option_trade_greeks(&self, records: &[OptionTradeGreeks]) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut count = 0;
        for record in records {
            sqlx::query(r#"
                INSERT INTO option_trade_greeks (
                    symbol, expiration, strike, right, timestamp, sequence, size, price, condition, exchange,
                    underlying_timestamp, underlying_price, implied_vol, iv_error,
                    delta, gamma, theta, vega, rho, epsilon, lambda_val,
                    vanna, charm, vomma, veta, color, zomma, speed, ultima
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                        $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
                ON CONFLICT (symbol, expiration, strike, right, sequence) DO NOTHING
            "#)
            .bind(&record.symbol)
            .bind(record.expiration)
            .bind(record.strike)
            .bind(&record.right)
            .bind(record.timestamp)
            .bind(record.sequence)
            .bind(record.size)
            .bind(record.price)
            .bind(record.condition)
            .bind(record.exchange)
            .bind(record.underlying_timestamp)
            .bind(record.underlying_price)
            .bind(record.implied_vol)
            .bind(record.iv_error)
            .bind(record.delta)
            .bind(record.gamma)
            .bind(record.theta)
            .bind(record.vega)
            .bind(record.rho)
            .bind(record.epsilon)
            .bind(record.lambda_val)
            .bind(record.vanna)
            .bind(record.charm)
            .bind(record.vomma)
            .bind(record.veta)
            .bind(record.color)
            .bind(record.zomma)
            .bind(record.speed)
            .bind(record.ultima)
            .execute(&self.pool)
            .await?;
            count += 1;
        }

        Ok(count)
    }

    /// Bulk insert option open interest records
    pub async fn insert_option_open_interest(&self, records: &[OptionOpenInterest]) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut count = 0;
        for record in records {
            sqlx::query(r#"
                INSERT INTO option_open_interest (symbol, expiration, strike, right, trade_date, open_interest)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (symbol, expiration, strike, right, trade_date) DO UPDATE SET
                    open_interest = EXCLUDED.open_interest
            "#)
            .bind(&record.symbol)
            .bind(record.expiration)
            .bind(record.strike)
            .bind(&record.right)
            .bind(record.trade_date)
            .bind(record.open_interest)
            .execute(&self.pool)
            .await?;
            count += 1;
        }

        Ok(count)
    }

    // Index insert methods

    /// Bulk insert index OHLC records
    pub async fn insert_index_ohlc(&self, records: &[IndexOhlc]) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut count = 0;
        for record in records {
            sqlx::query(r#"
                INSERT INTO index_ohlc (symbol, timestamp, open, high, low, close, volume, count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    count = EXCLUDED.count
            "#)
            .bind(&record.symbol)
            .bind(record.timestamp)
            .bind(record.open)
            .bind(record.high)
            .bind(record.low)
            .bind(record.close)
            .bind(record.volume)
            .bind(record.count)
            .execute(&self.pool)
            .await?;
            count += 1;
        }

        Ok(count)
    }

    /// Bulk insert index EOD records
    pub async fn insert_index_eod(&self, records: &[IndexEod]) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut count = 0;
        for record in records {
            sqlx::query(r#"
                INSERT INTO index_eod (symbol, trade_date, report_created, last_trade, open, high, low, close, volume, count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (symbol, trade_date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    count = EXCLUDED.count
            "#)
            .bind(&record.symbol)
            .bind(record.trade_date)
            .bind(record.report_created)
            .bind(record.last_trade)
            .bind(record.open)
            .bind(record.high)
            .bind(record.low)
            .bind(record.close)
            .bind(record.volume)
            .bind(record.count)
            .execute(&self.pool)
            .await?;
            count += 1;
        }

        Ok(count)
    }

    /// Get record count for a table
    pub async fn get_count(&self, table: &str, symbol: Option<&str>) -> Result<i64> {
        let query = if let Some(sym) = symbol {
            format!("SELECT COUNT(*) FROM {} WHERE symbol = '{}'", table, sym)
        } else {
            format!("SELECT COUNT(*) FROM {}", table)
        };

        let row = sqlx::query(&query)
            .fetch_one(&self.pool)
            .await?;

        Ok(row.get(0))
    }
}
