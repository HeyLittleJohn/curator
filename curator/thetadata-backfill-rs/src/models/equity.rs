//! Equity data models.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Equity OHLC data
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityOhlc {
    #[serde(skip_deserializing)]
    pub id: Option<i64>,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: i64,
    pub count: Option<i64>,
}

/// Equity trade data
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityTrade {
    #[serde(skip_deserializing)]
    pub id: Option<i64>,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub sequence: i64,
    pub size: i64,
    pub price: Decimal,
    pub condition: i32,
    pub exchange: Option<i32>,
    pub ext_condition1: Option<i32>,
    pub ext_condition2: Option<i32>,
    pub ext_condition3: Option<i32>,
    pub ext_condition4: Option<i32>,
}

/// Equity quote (NBBO) data
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityQuote {
    #[serde(skip_deserializing)]
    pub id: Option<i64>,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub bid: Decimal,
    pub bid_size: i64,
    pub bid_exchange: Option<i32>,
    pub bid_condition: Option<i32>,
    pub ask: Decimal,
    pub ask_size: i64,
    pub ask_exchange: Option<i32>,
    pub ask_condition: Option<i32>,
}

/// Equity end-of-day report
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EquityEod {
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
    pub bid: Option<Decimal>,
    pub bid_size: Option<i64>,
    pub ask: Option<Decimal>,
    pub ask_size: Option<i64>,
}

/// Equity OHLC API response
#[derive(Debug, Clone, Deserialize)]
pub struct EquityOhlcResponse {
    pub timestamp: DateTime<Utc>,
    pub symbol: Option<String>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: i64,
    pub count: Option<i64>,
}

impl EquityOhlcResponse {
    pub fn to_record(self, symbol: &str) -> EquityOhlc {
        EquityOhlc {
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

/// Equity trade API response
#[derive(Debug, Clone, Deserialize)]
pub struct EquityTradeResponse {
    pub timestamp: DateTime<Utc>,
    pub symbol: Option<String>,
    pub sequence: i64,
    pub size: i64,
    pub price: Decimal,
    pub condition: i32,
    pub exchange: Option<i32>,
    pub ext_condition1: Option<i32>,
    pub ext_condition2: Option<i32>,
    pub ext_condition3: Option<i32>,
    pub ext_condition4: Option<i32>,
}

impl EquityTradeResponse {
    pub fn to_record(self, symbol: &str) -> EquityTrade {
        EquityTrade {
            id: None,
            symbol: self.symbol.unwrap_or_else(|| symbol.to_string()),
            timestamp: self.timestamp,
            sequence: self.sequence,
            size: self.size,
            price: self.price,
            condition: self.condition,
            exchange: self.exchange,
            ext_condition1: self.ext_condition1,
            ext_condition2: self.ext_condition2,
            ext_condition3: self.ext_condition3,
            ext_condition4: self.ext_condition4,
        }
    }
}

/// Equity quote API response
#[derive(Debug, Clone, Deserialize)]
pub struct EquityQuoteResponse {
    pub timestamp: DateTime<Utc>,
    pub symbol: Option<String>,
    pub bid: Decimal,
    pub bid_size: i64,
    pub bid_exchange: Option<i32>,
    pub bid_condition: Option<i32>,
    pub ask: Decimal,
    pub ask_size: i64,
    pub ask_exchange: Option<i32>,
    pub ask_condition: Option<i32>,
}

impl EquityQuoteResponse {
    pub fn to_record(self, symbol: &str) -> EquityQuote {
        EquityQuote {
            id: None,
            symbol: self.symbol.unwrap_or_else(|| symbol.to_string()),
            timestamp: self.timestamp,
            bid: self.bid,
            bid_size: self.bid_size,
            bid_exchange: self.bid_exchange,
            bid_condition: self.bid_condition,
            ask: self.ask,
            ask_size: self.ask_size,
            ask_exchange: self.ask_exchange,
            ask_condition: self.ask_condition,
        }
    }
}

/// Equity EOD API response
#[derive(Debug, Clone, Deserialize)]
pub struct EquityEodResponse {
    pub created: DateTime<Utc>,
    pub last_trade: Option<DateTime<Utc>>,
    pub symbol: Option<String>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: i64,
    #[serde(default)]
    pub count: i64,
    pub bid: Option<Decimal>,
    pub bid_size: Option<i64>,
    pub ask: Option<Decimal>,
    pub ask_size: Option<i64>,
}

impl EquityEodResponse {
    pub fn to_record(self, symbol: &str) -> EquityEod {
        EquityEod {
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
            bid: self.bid,
            bid_size: self.bid_size,
            ask: self.ask,
            ask_size: self.ask_size,
        }
    }
}
