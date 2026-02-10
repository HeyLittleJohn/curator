//! Index data downloaders.

use async_trait::async_trait;
use chrono::NaiveDate;

use crate::client::ThetaDataClient;
use crate::db::Database;
use crate::error::Result;
use crate::models::*;

use super::traits::Downloader;

/// Index OHLC downloader
pub struct IndexOhlcDownloader;

#[async_trait]
impl Downloader for IndexOhlcDownloader {
    fn asset_type(&self) -> AssetType {
        AssetType::Index
    }

    fn data_type(&self) -> DataType {
        DataType::Ohlc
    }

    fn table_name(&self) -> String {
        "index_ohlc".to_string()
    }

    async fn get_symbols(&self, client: &ThetaDataClient) -> Result<Vec<String>> {
        client.get_index_symbols().await
    }

    async fn get_dates(&self, client: &ThetaDataClient, symbol: &str) -> Result<Vec<NaiveDate>> {
        client.get_index_dates(symbol).await
    }

    async fn download_date(
        &self,
        client: &ThetaDataClient,
        db: &Database,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<usize> {
        let date_str = date.format("%Y%m%d").to_string();
        let responses: Vec<IndexOhlcResponse> = client.get(
            "index/history/ohlc",
            &[
                ("symbol", symbol),
                ("date", &date_str),
                ("interval", "1m"),
            ],
        ).await?;

        let records: Vec<IndexOhlc> = responses
            .into_iter()
            .map(|r| r.to_record(symbol))
            .collect();

        db.insert_index_ohlc(&records).await
    }
}

/// Index EOD downloader
pub struct IndexEodDownloader;

#[async_trait]
impl Downloader for IndexEodDownloader {
    fn asset_type(&self) -> AssetType {
        AssetType::Index
    }

    fn data_type(&self) -> DataType {
        DataType::Eod
    }

    fn table_name(&self) -> String {
        "index_eod".to_string()
    }

    async fn get_symbols(&self, client: &ThetaDataClient) -> Result<Vec<String>> {
        client.get_index_symbols().await
    }

    async fn get_dates(&self, client: &ThetaDataClient, symbol: &str) -> Result<Vec<NaiveDate>> {
        client.get_index_dates(symbol).await
    }

    async fn download_date(
        &self,
        client: &ThetaDataClient,
        db: &Database,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<usize> {
        let date_str = date.format("%Y%m%d").to_string();
        let responses: Vec<IndexEodResponse> = client.get(
            "index/history/eod",
            &[
                ("symbol", symbol),
                ("start_date", &date_str),
                ("end_date", &date_str),
            ],
        ).await?;

        let records: Vec<IndexEod> = responses
            .into_iter()
            .map(|r| r.to_record(symbol))
            .collect();

        db.insert_index_eod(&records).await
    }
}
