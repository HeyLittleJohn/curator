"""Futures operations library for candlestick aggregation and technical analysis.

This module provides query-time calculations for silver futures data including:
- Custom interval candlestick aggregation from the materialized view
- Bollinger Bands calculation
- RSI (Relative Strength Index)
- MACD (Moving Average Convergence Divergence)

Example usage:
    import asyncio
    from curator.src.futures_ops import get_candlesticks, calculate_bollinger_bands

    df = asyncio.run(get_candlesticks("SILH4", interval_minutes=5))
    df = calculate_bollinger_bands(df)
"""

from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import text

from curator.proj_constants import async_session_maker


async def get_candlesticks(
    symbol: str,
    interval_minutes: int = 1,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> pd.DataFrame:
    """Query candlestick data with custom interval aggregation.

    Fetches OHLCV data from the 1-minute materialized view and optionally
    aggregates into larger intervals.

    Args:
        symbol: Futures contract symbol (e.g., "SILH4", "SILZ4").
        interval_minutes: Candlestick interval in minutes (1, 5, 15, 30, 60, etc.).
        start_time: Optional start time filter.
        end_time: Optional end time filter.

    Returns:
        DataFrame with columns: period_start, open_price, high_price, low_price,
        close_price, volume, trade_count.
    """
    async with async_session_maker() as session:
        if interval_minutes == 1:
            # Direct query from 1-minute materialized view
            query = """
                SELECT 
                    period_start,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    trade_count
                FROM silver_futures_candlesticks_1m
                WHERE symbol = :symbol
            """
            params = {"symbol": symbol}

            if start_time:
                query += " AND period_start >= :start_time"
                params["start_time"] = start_time
            if end_time:
                query += " AND period_start <= :end_time"
                params["end_time"] = end_time

            query += " ORDER BY period_start"

        else:
            # Aggregate 1-minute candles into larger intervals
            query = """
                WITH grouped AS (
                    SELECT 
                        date_trunc('hour', period_start) + 
                            (EXTRACT(minute FROM period_start)::int / :interval) * :interval * INTERVAL '1 minute' 
                            AS period_start,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        trade_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY date_trunc('hour', period_start) + 
                                (EXTRACT(minute FROM period_start)::int / :interval) * :interval * INTERVAL '1 minute'
                            ORDER BY period_start
                        ) as rn_first,
                        ROW_NUMBER() OVER (
                            PARTITION BY date_trunc('hour', period_start) + 
                                (EXTRACT(minute FROM period_start)::int / :interval) * :interval * INTERVAL '1 minute'
                            ORDER BY period_start DESC
                        ) as rn_last
                    FROM silver_futures_candlesticks_1m
                    WHERE symbol = :symbol
            """
            params = {"symbol": symbol, "interval": interval_minutes}

            if start_time:
                query += " AND period_start >= :start_time"
                params["start_time"] = start_time
            if end_time:
                query += " AND period_start <= :end_time"
                params["end_time"] = end_time

            query += """
                )
                SELECT 
                    g.period_start,
                    MAX(CASE WHEN rn_first = 1 THEN open_price END) AS open_price,
                    MAX(high_price) AS high_price,
                    MIN(low_price) AS low_price,
                    MAX(CASE WHEN rn_last = 1 THEN close_price END) AS close_price,
                    SUM(volume) AS volume,
                    SUM(trade_count) AS trade_count
                FROM grouped g
                GROUP BY g.period_start
                ORDER BY g.period_start
            """

        result = await session.execute(text(query), params)
        rows = result.fetchall()

        if not rows:
            return pd.DataFrame(
                columns=[
                    "period_start",
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                    "volume",
                    "trade_count",
                ]
            )

        df = pd.DataFrame(
            rows,
            columns=["period_start", "open_price", "high_price", "low_price", "close_price", "volume", "trade_count"],
        )

        # Convert numeric columns to float
        for col in ["open_price", "high_price", "low_price", "close_price"]:
            df[col] = df[col].astype(float)

        return df


async def get_available_symbols() -> list[str]:
    """Get list of all available symbols in the candlestick data.

    Returns:
        List of unique symbol strings.
    """
    async with async_session_maker() as session:
        result = await session.execute(
            text("SELECT DISTINCT symbol FROM silver_futures_candlesticks_1m ORDER BY symbol")
        )
        return [row[0] for row in result.fetchall()]


async def refresh_candlestick_view() -> None:
    """Refresh the 1-minute candlestick materialized view.

    Should be called after uploading new MBO data to update the aggregations.
    """
    async with async_session_maker() as session:
        await session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY silver_futures_candlesticks_1m"))
        await session.commit()


def calculate_bollinger_bands(
    df: pd.DataFrame,
    window: int = 20,
    num_std: float = 2.0,
    price_column: str = "close_price",
) -> pd.DataFrame:
    """Calculate Bollinger Bands on candlestick data.

    Adds SMA (Simple Moving Average), upper band, lower band, and bandwidth
    columns to the DataFrame.

    Args:
        df: DataFrame with OHLCV candlestick data.
        window: Moving average period (default 20).
        num_std: Number of standard deviations for bands (default 2.0).
        price_column: Column to use for calculations (default "close_price").

    Returns:
        DataFrame with added columns: sma, upper_band, lower_band, bandwidth.
    """
    df = df.copy()

    # Simple Moving Average
    df["sma"] = df[price_column].rolling(window=window).mean()

    # Rolling Standard Deviation
    rolling_std = df[price_column].rolling(window=window).std()

    # Upper and Lower Bands
    df["upper_band"] = df["sma"] + (rolling_std * num_std)
    df["lower_band"] = df["sma"] - (rolling_std * num_std)

    # Bandwidth (percentage)
    df["bandwidth"] = ((df["upper_band"] - df["lower_band"]) / df["sma"]) * 100

    return df


def calculate_rsi(
    df: pd.DataFrame,
    period: int = 14,
    price_column: str = "close_price",
) -> pd.DataFrame:
    """Calculate Relative Strength Index (RSI).

    Args:
        df: DataFrame with OHLCV candlestick data.
        period: RSI period (default 14).
        price_column: Column to use for calculations (default "close_price").

    Returns:
        DataFrame with added column: rsi.
    """
    df = df.copy()

    # Price changes
    delta = df[price_column].diff()

    # Separate gains and losses
    gains = delta.where(delta > 0, 0.0)
    losses = (-delta).where(delta < 0, 0.0)

    # Average gains and losses (exponential moving average)
    avg_gains = gains.ewm(span=period, adjust=False).mean()
    avg_losses = losses.ewm(span=period, adjust=False).mean()

    # Relative Strength
    rs = avg_gains / avg_losses

    # RSI
    df["rsi"] = 100 - (100 / (1 + rs))

    return df


def calculate_macd(
    df: pd.DataFrame,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
    price_column: str = "close_price",
) -> pd.DataFrame:
    """Calculate MACD (Moving Average Convergence Divergence).

    Args:
        df: DataFrame with OHLCV candlestick data.
        fast_period: Fast EMA period (default 12).
        slow_period: Slow EMA period (default 26).
        signal_period: Signal line EMA period (default 9).
        price_column: Column to use for calculations (default "close_price").

    Returns:
        DataFrame with added columns: macd, macd_signal, macd_histogram.
    """
    df = df.copy()

    # Exponential Moving Averages
    ema_fast = df[price_column].ewm(span=fast_period, adjust=False).mean()
    ema_slow = df[price_column].ewm(span=slow_period, adjust=False).mean()

    # MACD Line
    df["macd"] = ema_fast - ema_slow

    # Signal Line
    df["macd_signal"] = df["macd"].ewm(span=signal_period, adjust=False).mean()

    # Histogram
    df["macd_histogram"] = df["macd"] - df["macd_signal"]

    return df


def calculate_atr(
    df: pd.DataFrame,
    period: int = 14,
) -> pd.DataFrame:
    """Calculate Average True Range (ATR).

    Args:
        df: DataFrame with OHLCV candlestick data (must have high, low, close).
        period: ATR period (default 14).

    Returns:
        DataFrame with added column: atr.
    """
    df = df.copy()

    # True Range components
    high_low = df["high_price"] - df["low_price"]
    high_close = abs(df["high_price"] - df["close_price"].shift(1))
    low_close = abs(df["low_price"] - df["close_price"].shift(1))

    # True Range is max of the three
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

    # Average True Range (EMA of True Range)
    df["atr"] = tr.ewm(span=period, adjust=False).mean()

    return df


def calculate_sma(
    df: pd.DataFrame,
    periods: list[int] = [20, 50, 200],
    price_column: str = "close_price",
) -> pd.DataFrame:
    """Calculate Simple Moving Averages for multiple periods.

    Args:
        df: DataFrame with OHLCV candlestick data.
        periods: List of SMA periods to calculate.
        price_column: Column to use for calculations.

    Returns:
        DataFrame with added columns: sma_{period} for each period.
    """
    df = df.copy()

    for period in periods:
        df[f"sma_{period}"] = df[price_column].rolling(window=period).mean()

    return df


def calculate_ema(
    df: pd.DataFrame,
    periods: list[int] = [12, 26, 50],
    price_column: str = "close_price",
) -> pd.DataFrame:
    """Calculate Exponential Moving Averages for multiple periods.

    Args:
        df: DataFrame with OHLCV candlestick data.
        periods: List of EMA periods to calculate.
        price_column: Column to use for calculations.

    Returns:
        DataFrame with added columns: ema_{period} for each period.
    """
    df = df.copy()

    for period in periods:
        df[f"ema_{period}"] = df[price_column].ewm(span=period, adjust=False).mean()

    return df
