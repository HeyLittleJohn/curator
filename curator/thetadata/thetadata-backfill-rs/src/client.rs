//! Async HTTP client for ThetaData API.

use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveDate;
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::config::Config;
use crate::error::{BackfillError, Result};

/// ThetaData API client with rate limiting and retries
pub struct ThetaDataClient {
    client: Client,
    base_url: String,
    semaphore: Arc<Semaphore>,
    max_retries: u32,
}

impl ThetaDataClient {
    /// Create a new client from configuration
    pub fn new(config: &Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.api_timeout))
            .pool_max_idle_per_host(config.api_rate_limit)
            .gzip(true)
            .build()
            .map_err(BackfillError::Http)?;

        Ok(Self {
            client,
            base_url: config.api_base_url.trim_end_matches('/').to_string(),
            semaphore: Arc::new(Semaphore::new(config.api_rate_limit)),
            max_retries: config.api_max_retries,
        })
    }

    /// Make a GET request with rate limiting and retries
    pub async fn get<T: DeserializeOwned>(&self, endpoint: &str, params: &[(&str, &str)]) -> Result<Vec<T>> {
        let url = format!("{}/{}", self.base_url, endpoint.trim_start_matches('/'));
        
        // Add format=json parameter
        let mut all_params: Vec<(&str, &str)> = params.to_vec();
        all_params.push(("format", "json"));

        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            // Acquire semaphore permit for rate limiting
            let _permit = self.semaphore.acquire().await.unwrap();

            match self.do_request::<T>(&url, &all_params).await {
                Ok(data) => return Ok(data),
                Err(BackfillError::RateLimit { retry_after }) => {
                    warn!("Rate limited, waiting {}s", retry_after);
                    tokio::time::sleep(Duration::from_secs(retry_after)).await;
                    last_error = Some(BackfillError::RateLimit { retry_after });
                }
                Err(BackfillError::Http(e)) if e.is_timeout() || e.is_connect() => {
                    let wait = 2u64.pow(attempt);
                    warn!("Request failed, retrying in {}s: {}", wait, e);
                    tokio::time::sleep(Duration::from_secs(wait)).await;
                    last_error = Some(BackfillError::Http(e));
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_error.unwrap_or_else(|| BackfillError::Task("Max retries exceeded".to_string())))
    }

    async fn do_request<T: DeserializeOwned>(&self, url: &str, params: &[(&str, &str)]) -> Result<Vec<T>> {
        debug!("GET {}", url);

        let response = self.client
            .get(url)
            .query(params)
            .send()
            .await?;

        let status = response.status();

        if status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(1);
            return Err(BackfillError::RateLimit { retry_after });
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(BackfillError::Api {
                status: status.as_u16(),
                message: body,
            });
        }

        let json: Value = response.json().await?;
        
        // ThetaData wraps responses in a "response" key
        let response_array = if let Some(arr) = json.get("response") {
            arr.clone()
        } else {
            json
        };

        let items: Vec<T> = serde_json::from_value(response_array)?;
        Ok(items)
    }

    /// Get raw JSON response for complex parsing
    pub async fn get_raw(&self, endpoint: &str, params: &[(&str, &str)]) -> Result<Value> {
        let url = format!("{}/{}", self.base_url, endpoint.trim_start_matches('/'));
        
        let mut all_params: Vec<(&str, &str)> = params.to_vec();
        all_params.push(("format", "json"));

        let _permit = self.semaphore.acquire().await.unwrap();

        let response = self.client
            .get(&url)
            .query(&all_params)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(BackfillError::Api {
                status: status.as_u16(),
                message: body,
            });
        }

        let json: Value = response.json().await?;
        
        if let Some(arr) = json.get("response") {
            Ok(arr.clone())
        } else {
            Ok(json)
        }
    }

    // Convenience methods for common endpoints

    /// Get list of stock symbols
    pub async fn get_stock_symbols(&self) -> Result<Vec<String>> {
        #[derive(serde::Deserialize)]
        struct SymbolResponse {
            symbol: String,
        }
        
        let items: Vec<SymbolResponse> = self.get("stock/list/symbols", &[]).await?;
        Ok(items.into_iter().map(|s| s.symbol).collect())
    }

    /// Get available dates for a stock symbol
    pub async fn get_stock_dates(&self, symbol: &str, request_type: &str) -> Result<Vec<NaiveDate>> {
        #[derive(serde::Deserialize)]
        struct DateResponse {
            date: String,
        }
        
        let endpoint = format!("stock/list/dates/{}", request_type);
        let items: Vec<DateResponse> = self.get(&endpoint, &[("symbol", symbol)]).await?;
        
        items.into_iter()
            .map(|d| NaiveDate::parse_from_str(&d.date, "%Y-%m-%d")
                .map_err(|e| BackfillError::Parse(e.to_string())))
            .collect()
    }

    /// Get list of index symbols
    pub async fn get_index_symbols(&self) -> Result<Vec<String>> {
        #[derive(serde::Deserialize)]
        struct SymbolResponse {
            symbol: String,
        }
        
        let items: Vec<SymbolResponse> = self.get("index/list/symbols", &[]).await?;
        Ok(items.into_iter().map(|s| s.symbol).collect())
    }

    /// Get available dates for an index symbol
    pub async fn get_index_dates(&self, symbol: &str) -> Result<Vec<NaiveDate>> {
        #[derive(serde::Deserialize)]
        struct DateResponse {
            date: String,
        }
        
        let items: Vec<DateResponse> = self.get("index/list/dates", &[("symbol", symbol)]).await?;
        
        items.into_iter()
            .map(|d| NaiveDate::parse_from_str(&d.date, "%Y-%m-%d")
                .map_err(|e| BackfillError::Parse(e.to_string())))
            .collect()
    }

    /// Get option expirations for a symbol
    pub async fn get_option_expirations(&self, symbol: &str) -> Result<Vec<NaiveDate>> {
        #[derive(serde::Deserialize)]
        struct ExpirationResponse {
            expiration: String,
        }
        
        let items: Vec<ExpirationResponse> = self.get(
            "option/list/expirations",
            &[("symbol", symbol)],
        ).await?;
        
        items.into_iter()
            .map(|e| NaiveDate::parse_from_str(&e.expiration, "%Y-%m-%d")
                .map_err(|e| BackfillError::Parse(e.to_string())))
            .collect()
    }
}
