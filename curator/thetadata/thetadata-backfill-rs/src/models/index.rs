//! Index data models.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Index OHLC data
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct IndexOhlc {
    #[serde(skip_deserializing)]
    pub id: Option<i64>,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Option<i64>,
    pub count: Option<i64>,
}

/// Index price tick data
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct IndexPrice {
    #[serde(skip_deserializing)]
    pub id: Option<i64>,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub price: Decimal,
}

/// Index end-of-day report
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct IndexEod {
    #[serde(skip_deserializing)]
    pub id: Option<i64>,
    pub symbol: String,
    pub trade_date: NaiveDate,
    pub report_created: DateTime<Utc>,
    pub last_trade: Option<DateTime<Utc>>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: i64,
    pub count: i64,
}

/// Index OHLC API response
#[derive(Debug, Clone, Deserialize)]
pub struct IndexOhlcResponse {
    pub timestamp: DateTime<Utc>,
    pub symbol: Option<String>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Option<i64>,
    pub count: Option<i64>,
}

impl IndexOhlcResponse {
    pub fn to_record(self, symbol: &str) -> IndexOhlc {
        IndexOhlc {
            id: None,
            symbol: self.symbol.unwrap_or_else(|| symbol.to_string()),
            timestamp: self.timestamp,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            count: self.count,
        }
    }
}

/// Index price API response
#[derive(Debug, Clone, Deserialize)]
pub struct IndexPriceResponse {
    pub timestamp: DateTime<Utc>,
    pub symbol: Option<String>,
    pub price: Decimal,
}

impl IndexPriceResponse {
    pub fn to_record(self, symbol: &str) -> IndexPrice {
        IndexPrice {
            id: None,
            symbol: self.symbol.unwrap_or_else(|| symbol.to_string()),
            timestamp: self.timestamp,
            price: self.price,
        }
    }
}

/// Index EOD API response
#[derive(Debug, Clone, Deserialize)]
pub struct IndexEodResponse {
    pub created: DateTime<Utc>,
    pub last_trade: Option<DateTime<Utc>>,
    pub symbol: Option<String>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    #[serde(default)]
    pub volume: i64,
    #[serde(default)]
    pub count: i64,
}

impl IndexEodResponse {
    pub fn to_record(self, symbol: &str) -> IndexEod {
        IndexEod {
            id: None,
            symbol: self.symbol.unwrap_or_else(|| symbol.to_string()),
            trade_date: self.created.date_naive(),
            report_created: self.created,
            last_trade: self.last_trade,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            count: self.count,
        }
    }
}
