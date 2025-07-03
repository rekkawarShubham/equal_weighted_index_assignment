import os
import sys
import duckdb
import polars as pl
import pandas as pd
from datetime import date, timedelta
import yfinance as yf
from dotenv import load_dotenv
import time
from typing import List, Optional

# Load environment variables
load_dotenv()

# Add the parent directory to the system path to import modules from 'app'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from services.database import get_db_connection, initialize_db

# --- Configuration & Ticker List ---
# TICKERS_TO_INGEST = [
#     "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "V", "UNH"
# ]

TICKERS_TO_INGEST = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()
TICKERS_TO_INGEST = TICKERS_TO_INGEST[:200]
print("Fetching services for : ",len(TICKERS_TO_INGEST))
DAYS_HISTORY = 45


# --- Data Fetching and Transformation (using Pandas for yfinance, then convert to Polars) ---
def fetch_daily_ohlcv_and_market_cap(
        ticker: str, start_date: date, end_date: date
) -> Optional[pl.DataFrame]:
    try:
        # Step 1: Fetch services using yfinance (returns Pandas DataFrame)
        df_pandas = yf.download(
            ticker,
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            interval="1d",
        )

        if df_pandas is None or df_pandas.empty:
            return None

        # This ensures column names are flattened to simple strings before processing
        if isinstance(df_pandas.columns, pd.MultiIndex):
            # Assuming the structure is (Metric, TickerSymbol)
            # We want only the Metric (e.g., 'Open', 'High')
            df_pandas.columns = df_pandas.columns.get_level_values(0)
            # If flattening creates duplicate column names (e.g., 'Close' and 'Adj Close'
            # both become 'Close' if the original MultiIndex only had 'Close' as level 0),
            # this removes the duplicates, keeping the first.
            df_pandas = df_pandas.loc[:, ~df_pandas.columns.duplicated()]

        # Step 2: Perform initial transformations using Pandas
        df_pandas = df_pandas.rename(columns={
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",  # Use Adjusted Close for accuracy
            "Volume": "volume"
        })
        # Drop original 'Close' if 'Adj Close' was used
        if 'Close' in df_pandas.columns and 'Adj Close' in df_pandas.columns:
            df_pandas = df_pandas.drop(columns=['Close'])

        df_pandas['ticker'] = ticker
        df_pandas['trade_date'] = df_pandas.index.date  # Convert DatetimeIndex to date objects
        df_pandas['market_cap'] = df_pandas['close_price'] * df_pandas['volume']  # Approximate market cap

        # Step 3: Explicitly select and reorder columns (still crucial for consistency)
        final_columns = [
            'trade_date', 'ticker', 'open_price', 'high_price',
            'low_price', 'close_price', 'volume', 'market_cap'
        ]
        df_pandas = df_pandas[final_columns]

        # Step 4: Convert the Pandas DataFrame to a Polars DataFrame
        return pl.DataFrame(df_pandas)

    except Exception as e:
        print(f"Warning: Could not fetch services for {ticker} from {start_date} to {end_date}. Reason: {e}")
        return None


# --- Main Ingestion Logic (remains identical from previous step) ---
def run_data_ingestion_with_polars():
    initialize_db()
    conn = get_db_connection()

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=DAYS_HISTORY)

    print(f"Starting services ingestion from {start_date} to {end_date} for {len(TICKERS_TO_INGEST)} tickers.")

    all_polars_dataframes = []

    for ticker in TICKERS_TO_INGEST:
        print(f"  Fetching services for {ticker}...")
        df_polars = fetch_daily_ohlcv_and_market_cap(ticker, start_date, end_date)

        if df_polars is not None and not df_polars.is_empty():
            all_polars_dataframes.append(df_polars)
            print(f"  Successfully fetched {df_polars.shape[0]} records for {ticker}.")
        else:
            print(f"  No services or error fetching services for {ticker}. Check logs above for details.")

        time.sleep(2)

    if all_polars_dataframes:
        combined_df_polars = pl.concat(all_polars_dataframes, how="vertical_relaxed")
        print(f"All services combined: {combined_df_polars.shape[0]} records.")

        combined_df_polars = combined_df_polars.with_columns([
            pl.col("trade_date").cast(pl.Date),
            pl.col("ticker").cast(pl.String),
            pl.col("open_price").cast(pl.Float64),
            pl.col("high_price").cast(pl.Float64),
            pl.col("low_price").cast(pl.Float64),
            pl.col("close_price").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64),
            pl.col("market_cap").cast(pl.Float64)
        ])

        print("\n--- Debugging Combined Polars DataFrame ---")
        print(f"Shape: {combined_df_polars.shape}")
        print(f"Columns: {combined_df_polars.columns}")
        print(f"Schema:\n{combined_df_polars.schema}")
        print("-------------------------------------------\n")

        columns_for_insertion = [
            "trade_date", "ticker", "open_price", "high_price",
            "low_price", "close_price", "volume", "market_cap"
        ]

        try:
            conn.execute(f"""
                INSERT OR REPLACE INTO daily_stock_data ({', '.join(columns_for_insertion)})
                SELECT {', '.join(columns_for_insertion)}
                FROM combined_df_polars
            """)
            print(f"Successfully inserted/replaced {combined_df_polars.shape[0]} records into DuckDB.")
        except Exception as e:
            print(f"Error inserting combined services into DuckDB: {e}. Please check table schema and DataFrame columns.")
    else:
        print("No services was fetched for any ticker.")

    conn.close()
    print("Data ingestion with Polars complete.")


# --- Execute the ingestion ---
if __name__ == "__main__":
    run_data_ingestion_with_polars()