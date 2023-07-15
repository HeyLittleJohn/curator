import numpy as np
from numpy.lib.stride_tricks import sliding_window_view


def calc_log_returns(prices: np.ndarray) -> np.ndarray:
    """Calculates log returns for a given array of prices.
    Return index will be one less than the input price index."""
    return np.log(prices[1:] / prices[:-1])


def calc_pct_returns(prices: np.ndarray) -> np.ndarray:
    """Calculates percentage returns for a given array of prices.
    Return index will be one less than the input price index."""
    return prices[1:] / prices[:-1] - 1


def calc_volatility(returns: np.ndarray, period: int) -> np.ndarray:
    """Calculates volatility for a given array of returns."""
    return np.std(sliding_window_view(returns, period), axis=1) * np.sqrt(period)


def calc_correlation(array_1: np.ndarray, array_2: np.ndarray) -> np.ndarray:
    """Calculates the correlation between two arrays of returns."""
    return np.corrcoef(array_1, array_2)[0, 1]
