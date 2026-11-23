//! Equity data downloaders.

use async_trait::async_trait;
use chrono::NaiveDate;

use crate::client::ThetaDataClient;
use crate::db::Database;
use crate::error::Result;
use crate::models::*;

use super::traits::Downloader;

/// Equity OHLC downloader
pub struct EquityOhlcDownloader;

#[async_trait]
impl Downloader for EquityOhlcDownloader {
    fn asset_type(&self) -> AssetType {
        AssetType::Equity
    }

    fn data_type(&self) -> DataType {
        DataType::Ohlc
    }

    fn table_name(&self) -> String {
        "equity_ohlc".to_string()
    }

    async fn get_symbols(&self, client: &ThetaDataClient) -> Result<Vec<String>> {
        client.get_stock_symbols().await
    }

    async fn get_dates(&self, client: &ThetaDataClient, symbol: &str) -> Result<Vec<NaiveDate>> {
        client.get_stock_dates(symbol, "ohlc").await
    }

    async fn download_date(
        &self,
        client: &ThetaDataClient,
        db: &Database,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<usize> {
        let date_str = date.format("%Y%m%d").to_string();
        let responses: Vec<EquityOhlcResponse> = client.get(
            "stock/history/ohlc",
            &[
                ("symbol", symbol),
                ("date", &date_str),
                ("interval", "1m"),
            ],
        ).await?;

        let records: Vec<EquityOhlc> = responses
            .into_iter()
            .map(|r| r.to_record(symbol))
            .collect();

        db.insert_equity_ohlc(&records).await
    }
}

/// Equity trade downloader
pub struct EquityTradeDownloader;

#[async_trait]
impl Downloader for EquityTradeDownloader {
    fn asset_type(&self) -> AssetType {
        AssetType::Equity
    }

    fn data_type(&self) -> DataType {
        DataType::Trade
    }

    fn table_name(&self) -> String {
        "equity_trade".to_string()
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
        let responses: Vec<EquityTradeResponse> = client.get(
            "stock/history/trade",
            &[
                ("symbol", symbol),
                ("date", &date_str),
            ],
        ).await?;

        let records: Vec<EquityTrade> = responses
            .into_iter()
            .map(|r| r.to_record(symbol))
            .collect();

        db.insert_equity_trades(&records).await
    }
}

/// Equity EOD downloader
pub struct EquityEodDownloader;

#[async_trait]
impl Downloader for EquityEodDownloader {
    fn asset_type(&self) -> AssetType {
        AssetType::Equity
    }

    fn data_type(&self) -> DataType {
        DataType::Eod
    }

    fn table_name(&self) -> String {
        "equity_eod".to_string()
    }

    async fn get_symbols(&self, client: &ThetaDataClient) -> Result<Vec<String>> {
        client.get_stock_symbols().await
    }

    async fn get_dates(&self, client: &ThetaDataClient, symbol: &str) -> Result<Vec<NaiveDate>> {
        client.get_stock_dates(symbol, "eod").await
    }

    async fn download_date(
        &self,
        client: &ThetaDataClient,
        db: &Database,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<usize> {
        let date_str = date.format("%Y%m%d").to_string();
        let responses: Vec<EquityEodResponse> = client.get(
            "stock/history/eod",
            &[
                ("symbol", symbol),
                ("start_date", &date_str),
                ("end_date", &date_str),
            ],
        ).await?;

        let records: Vec<EquityEod> = responses
            .into_iter()
            .map(|r| r.to_record(symbol))
            .collect();

        db.insert_equity_eod(&records).await
    }
}
