//! Options data models with full Greeks support.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Option right (call or put)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OptionRight {
    Call,
    Put,
}

impl std::fmt::Display for OptionRight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OptionRight::Call => write!(f, "CALL"),
            OptionRight::Put => write!(f, "PUT"),
        }
    }
}

impl std::str::FromStr for OptionRight {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "CALL" | "C" => Ok(OptionRight::Call),
            "PUT" | "P" => Ok(OptionRight::Put),
            _ => Err(format!("Invalid option right: {}", s)),
        }
    }
}

/// Option OHLC data
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OptionOhlc {
    #[serde(skip_deserializing)]
    pub id: Option<i64>,
    pub symbol: String,
    pub expiration: NaiveDate,
    pub strike: Decimal,
    pub right: String,
    pub timestamp: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: i64,
    pub count: Option<i64>,
}

/// Option trade with all Greeks
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OptionTradeGreeks {
    #[serde(skip_deserializing)]
    pub id: Option<i64>,
    pub symbol: String,
    pub expiration: NaiveDate,
    pub strike: Decimal,
    pub right: String,
    pub timestamp: DateTime<Utc>,
    pub sequence: i64,
    pub size: i64,
    pub price: Decimal,
    pub condition: i32,
    pub exchange: Option<i32>,
    pub underlying_timestamp: Option<DateTime<Utc>>,
    pub underlying_price: Option<Decimal>,
    pub implied_vol: Option<Decimal>,
    pub iv_error: Option<Decimal>,
    // First-order Greeks
    pub delta: Option<Decimal>,
    pub gamma: Option<Decimal>,
    pub theta: Option<Decimal>,
    pub vega: Option<Decimal>,
    pub rho: Option<Decimal>,
    pub epsilon: Option<Decimal>,
    pub lambda_val: Option<Decimal>,
    // Second-order Greeks
    pub vanna: Option<Decimal>,
    pub charm: Option<Decimal>,
    pub vomma: Option<Decimal>,
    pub veta: Option<Decimal>,
    pub color: Option<Decimal>,
    pub zomma: Option<Decimal>,
    pub speed: Option<Decimal>,
    pub ultima: Option<Decimal>,
}

/// Option Greeks (quote-based)
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OptionGreeks {
    #[serde(skip_deserializing)]
    pub id: Option<i64>,
    pub symbol: String,
    pub expiration: NaiveDate,
    pub strike: Decimal,
    pub right: String,
    pub timestamp: DateTime<Utc>,
    pub underlying_timestamp: Option<DateTime<Utc>>,
    pub bid: Decimal,
    pub ask: Decimal,
    pub underlying_price: Option<Decimal>,
    pub implied_vol: Option<Decimal>,
    pub iv_error: Option<Decimal>,
    // First-order Greeks
    pub delta: Option<Decimal>,
    pub gamma: Option<Decimal>,
    pub theta: Option<Decimal>,
    pub vega: Option<Decimal>,
    pub rho: Option<Decimal>,
    pub epsilon: Option<Decimal>,
    pub lambda_val: Option<Decimal>,
    // Second-order Greeks
    pub vanna: Option<Decimal>,
    pub charm: Option<Decimal>,
    pub vomma: Option<Decimal>,
    pub veta: Option<Decimal>,
    pub color: Option<Decimal>,
    pub zomma: Option<Decimal>,
    pub speed: Option<Decimal>,
    pub ultima: Option<Decimal>,
}

/// Option open interest
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OptionOpenInterest {
    #[serde(skip_deserializing)]
    pub id: Option<i64>,
    pub symbol: String,
    pub expiration: NaiveDate,
    pub strike: Decimal,
    pub right: String,
    pub trade_date: NaiveDate,
    pub open_interest: i64,
}

/// Option contract identifier (from API response)
#[derive(Debug, Clone, Deserialize)]
pub struct OptionContract {
    pub symbol: String,
    pub expiration: String,
    pub strike: Decimal,
    pub right: String,
}

/// Option trade Greeks API response
#[derive(Debug, Clone, Deserialize)]
pub struct OptionTradeGreeksResponse {
    pub timestamp: DateTime<Utc>,
    pub sequence: i64,
    pub size: i64,
    pub price: Decimal,
    pub condition: i32,
    pub exchange: Option<i32>,
    pub underlying_timestamp: Option<DateTime<Utc>>,
    pub underlying_price: Option<Decimal>,
    pub implied_vol: Option<Decimal>,
    pub iv_error: Option<Decimal>,
    // First-order Greeks
    pub delta: Option<Decimal>,
    pub gamma: Option<Decimal>,
    pub theta: Option<Decimal>,
    pub vega: Option<Decimal>,
    pub rho: Option<Decimal>,
    pub epsilon: Option<Decimal>,
    #[serde(rename = "lambda")]
    pub lambda_val: Option<Decimal>,
    // Second-order Greeks
    pub vanna: Option<Decimal>,
    pub charm: Option<Decimal>,
    pub vomma: Option<Decimal>,
    pub veta: Option<Decimal>,
    pub color: Option<Decimal>,
    pub zomma: Option<Decimal>,
    pub speed: Option<Decimal>,
    pub ultima: Option<Decimal>,
}

impl OptionTradeGreeksResponse {
    pub fn to_record(self, contract: &OptionContract) -> OptionTradeGreeks {
        OptionTradeGreeks {
            id: None,
            symbol: contract.symbol.clone(),
            expiration: NaiveDate::parse_from_str(&contract.expiration, "%Y-%m-%d")
                .unwrap_or_default(),
            strike: contract.strike,
            right: contract.right.to_uppercase(),
            timestamp: self.timestamp,
            sequence: self.sequence,
            size: self.size,
            price: self.price,
            condition: self.condition,
            exchange: self.exchange,
            underlying_timestamp: self.underlying_timestamp,
            underlying_price: self.underlying_price,
            implied_vol: self.implied_vol,
            iv_error: self.iv_error,
            delta: self.delta,
            gamma: self.gamma,
            theta: self.theta,
            vega: self.vega,
            rho: self.rho,
            epsilon: self.epsilon,
            lambda_val: self.lambda_val,
            vanna: self.vanna,
            charm: self.charm,
            vomma: self.vomma,
            veta: self.veta,
            color: self.color,
            zomma: self.zomma,
            speed: self.speed,
            ultima: self.ultima,
        }
    }
}

/// Option Greeks API response
#[derive(Debug, Clone, Deserialize)]
pub struct OptionGreeksResponse {
    pub timestamp: DateTime<Utc>,
    pub underlying_timestamp: Option<DateTime<Utc>>,
    pub bid: Decimal,
    pub ask: Decimal,
    pub underlying_price: Option<Decimal>,
    pub implied_vol: Option<Decimal>,
    pub iv_error: Option<Decimal>,
    // First-order Greeks
    pub delta: Option<Decimal>,
    pub gamma: Option<Decimal>,
    pub theta: Option<Decimal>,
    pub vega: Option<Decimal>,
    pub rho: Option<Decimal>,
    pub epsilon: Option<Decimal>,
    #[serde(rename = "lambda")]
    pub lambda_val: Option<Decimal>,
    // Second-order Greeks
    pub vanna: Option<Decimal>,
    pub charm: Option<Decimal>,
    pub vomma: Option<Decimal>,
    pub veta: Option<Decimal>,
    pub color: Option<Decimal>,
    pub zomma: Option<Decimal>,
    pub speed: Option<Decimal>,
    pub ultima: Option<Decimal>,
}

impl OptionGreeksResponse {
    pub fn to_record(self, contract: &OptionContract) -> OptionGreeks {
        OptionGreeks {
            id: None,
            symbol: contract.symbol.clone(),
            expiration: NaiveDate::parse_from_str(&contract.expiration, "%Y-%m-%d")
                .unwrap_or_default(),
            strike: contract.strike,
            right: contract.right.to_uppercase(),
            timestamp: self.timestamp,
            underlying_timestamp: self.underlying_timestamp,
            bid: self.bid,
            ask: self.ask,
            underlying_price: self.underlying_price,
            implied_vol: self.implied_vol,
            iv_error: self.iv_error,
            delta: self.delta,
            gamma: self.gamma,
            theta: self.theta,
            vega: self.vega,
            rho: self.rho,
            epsilon: self.epsilon,
            lambda_val: self.lambda_val,
            vanna: self.vanna,
            charm: self.charm,
            vomma: self.vomma,
            veta: self.veta,
            color: self.color,
            zomma: self.zomma,
            speed: self.speed,
            ultima: self.ultima,
        }
    }
}

/// Nested API response with contract and data
#[derive(Debug, Clone, Deserialize)]
pub struct NestedOptionResponse<T> {
    pub contract: OptionContract,
    pub data: Vec<T>,
}
