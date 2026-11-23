//! Options data downloaders with Greeks.

use async_trait::async_trait;
use chrono::NaiveDate;
use serde::Deserialize;

use crate::client::ThetaDataClient;
use crate::db::Database;
use crate::error::Result;
use crate::models::*;

use super::traits::Downloader;

/// Option trade Greeks downloader (includes all first and second order Greeks)
pub struct OptionTradeGreeksDownloader;

#[async_trait]
impl Downloader for OptionTradeGreeksDownloader {
    fn asset_type(&self) -> AssetType {
        AssetType::Option
    }

    fn data_type(&self) -> DataType {
        DataType::TradeGreeks
    }

    fn table_name(&self) -> String {
        "option_trade_greeks".to_string()
    }

    async fn get_symbols(&self, client: &ThetaDataClient) -> Result<Vec<String>> {
        client.get_stock_symbols().await
    }

    async fn get_dates(&self, client: &ThetaDataClient, symbol: &str) -> Result<Vec<NaiveDate>> {
        client.get_stock_dates(symbol, "trade").await
    }

    async fn download_date(
        &self,
        client: &ThetaDataClient,
        db: &Database,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<usize> {
        let date_str = date.format("%Y%m%d").to_string();
        
        // Get raw response for nested parsing
        let json = client.get_raw(
            "option/history/trade_greeks/all",
            &[
                ("symbol", symbol),
                ("date", &date_str),
                ("expiration", "*"),
            ],
        ).await?;

        // Parse nested response
        let nested: Vec<NestedOptionResponse<OptionTradeGreeksResponse>> = 
            serde_json::from_value(json)?;

        let mut records = Vec::new();
        for item in nested {
            for data in item.data {
                records.push(data.to_record(&item.contract));
            }
        }

        db.insert_option_trade_greeks(&records).await
    }
}

/// Option Greeks downloader (quote-based Greeks)
pub struct OptionGreeksDownloader;

#[async_trait]
impl Downloader for OptionGreeksDownloader {
    fn asset_type(&self) -> AssetType {
        AssetType::Option
    }

    fn data_type(&self) -> DataType {
        DataType::Greeks
    }

    fn table_name(&self) -> String {
        "option_greeks".to_string()
    }

    async fn get_symbols(&self, client: &ThetaDataClient) -> Result<Vec<String>> {
        client.get_stock_symbols().await
    }

    async fn get_dates(&self, client: &ThetaDataClient, symbol: &str) -> Result<Vec<NaiveDate>> {
        client.get_stock_dates(symbol, "quote").await
    }

    async fn download_date(
        &self,
        client: &ThetaDataClient,
        _db: &Database,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<usize> {
        let date_str = date.format("%Y%m%d").to_string();
        
        let json = client.get_raw(
            "option/history/greeks/all",
            &[
                ("symbol", symbol),
                ("date", &date_str),
                ("expiration", "*"),
                ("interval", "5m"),
            ],
        ).await?;

        let nested: Vec<NestedOptionResponse<OptionGreeksResponse>> = 
            serde_json::from_value(json)?;

        let mut records = Vec::new();
        for item in nested {
            for data in item.data {
                records.push(data.to_record(&item.contract));
            }
        }

        // Note: Would insert to option_greeks table
        Ok(records.len())
    }
}

/// Option open interest downloader
pub struct OptionOpenInterestDownloader;

#[derive(Debug, Deserialize)]
struct OpenInterestData {
    date: String,
    open_interest: i64,
}

#[async_trait]
impl Downloader for OptionOpenInterestDownloader {
    fn asset_type(&self) -> AssetType {
        AssetType::Option
    }

    fn data_type(&self) -> DataType {
        DataType::OpenInterest
    }

    fn table_name(&self) -> String {
        "option_open_interest".to_string()
    }

    async fn get_symbols(&self, client: &ThetaDataClient) -> Result<Vec<String>> {
        client.get_stock_symbols().await
    }

    async fn get_dates(&self, client: &ThetaDataClient, symbol: &str) -> Result<Vec<NaiveDate>> {
        client.get_stock_dates(symbol, "open_interest").await
    }

    async fn download_date(
        &self,
        client: &ThetaDataClient,
        db: &Database,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<usize> {
        let date_str = date.format("%Y%m%d").to_string();
        
        let json = client.get_raw(
            "option/history/open_interest",
            &[
                ("symbol", symbol),
                ("date", &date_str),
                ("expiration", "*"),
            ],
        ).await?;

        let nested: Vec<NestedOptionResponse<OpenInterestData>> = 
            serde_json::from_value(json)?;

        let mut records = Vec::new();
        for item in nested {
            for data in item.data {
                let trade_date = NaiveDate::parse_from_str(&data.date, "%Y-%m-%d")
                    .unwrap_or(date);
                records.push(OptionOpenInterest {
                    id: None,
                    symbol: item.contract.symbol.clone(),
                    expiration: NaiveDate::parse_from_str(&item.contract.expiration, "%Y-%m-%d")
                        .unwrap_or_default(),
                    strike: item.contract.strike,
                    right: item.contract.right.to_uppercase(),
                    trade_date,
                    open_interest: data.open_interest,
                });
            }
        }

        db.insert_option_open_interest(&records).await
    }
}
