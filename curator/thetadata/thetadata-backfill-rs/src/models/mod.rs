//! Data models for the backfill system.

pub mod equity;
pub mod option;
pub mod index;

pub use equity::*;
pub use option::*;
pub use index::*;

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Common trait for all API response types
pub trait ApiResponse: Sized {
    /// Parse from JSON response
    fn from_json(json: serde_json::Value) -> crate::error::Result<Vec<Self>>;
}

/// Asset type for backfill operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AssetType {
    Equity,
    Option,
    Index,
}

impl std::fmt::Display for AssetType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AssetType::Equity => write!(f, "equity"),
            AssetType::Option => write!(f, "option"),
            AssetType::Index => write!(f, "index"),
        }
    }
}

/// Data type for backfill operations
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Ohlc,
    Trade,
    Quote,
    Eod,
    TradeGreeks,
    Greeks,
    OpenInterest,
    Price,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Ohlc => write!(f, "ohlc"),
            DataType::Trade => write!(f, "trade"),
            DataType::Quote => write!(f, "quote"),
            DataType::Eod => write!(f, "eod"),
            DataType::TradeGreeks => write!(f, "trade_greeks"),
            DataType::Greeks => write!(f, "greeks"),
            DataType::OpenInterest => write!(f, "open_interest"),
            DataType::Price => write!(f, "price"),
        }
    }
}

impl std::str::FromStr for DataType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ohlc" => Ok(DataType::Ohlc),
            "trade" => Ok(DataType::Trade),
            "quote" => Ok(DataType::Quote),
            "eod" => Ok(DataType::Eod),
            "trade_greeks" => Ok(DataType::TradeGreeks),
            "greeks" => Ok(DataType::Greeks),
            "open_interest" => Ok(DataType::OpenInterest),
            "price" => Ok(DataType::Price),
            _ => Err(format!("Unknown data type: {}", s)),
        }
    }
}
