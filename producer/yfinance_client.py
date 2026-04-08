from __future__ import annotations

import io
import requests
import pandas as pd
import yfinance as yf

def fetch_daily_prices(ticker: str) -> pd.DataFrame:
    """
    Fetch daily price data using yfinance.
    Returns a DataFrame with columns: date, open, high, low, close, volume
    """
    df = yf.download(ticker, period="1y", interval="1d", progress=False)
    
    if df.empty:
        raise ValueError(f"No data returned for ticker={ticker}")
    
    # Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df.columns = [c.strip().lower() for c in df.columns]
    
    # Rename 'adj close' if present
    if "adj close" in df.columns:
        df = df.drop(columns=["adj close"], errors="ignore")

    return df

