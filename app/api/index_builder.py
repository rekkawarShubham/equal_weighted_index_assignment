from fastapi import APIRouter, Query
import duckdb
import pandas as pd
from redis import Redis

router = APIRouter()
DB_PATH = "data/index_data.duckdb"
r = Redis(host="redis", port=6379)

@router.post("/build-index")
def build_index(start_date: str = Query(...), end_date: str = Query(None)):
    """
        Builds the equal-weighted index between the given start and end dates.

        For each day in the range:
        - Selects the top 100 stocks by market capitalization.
        - Assigns each a fixed weight of 1% (equal weighting).
        - Stores the index composition and calculates index returns.
        - Index performance (daily and cumulative return) is calculated and stored.
        - Redis is used to cache performance and daily compositions.

        Args:
            start_date (str): (format: YYYY-MM-DD).
            end_date (str): (format: YYYY-MM-DD).

        Returns:
            dict: A success message including the date range processed.
    """

    end_date = end_date or start_date
    con = duckdb.connect(DB_PATH)

    df = con.execute(f"""
        SELECT * FROM daily_stock_data
        WHERE trade_date BETWEEN '{start_date}' AND '{end_date}'
    """).fetchdf()
    print(len(df))
    print(df.columns)
    if df.empty:
        return {"message": "No stock services found for the given dates"}

    index_perf = []
    compositions = []

    for date in sorted(df["trade_date"].unique()):
        day_df = df[df["trade_date"] == date].nlargest(100, "market_cap")
        day_df["weight"] = 1 / 100

        compositions.extend([
            {"trade_date": date, "ticker": row["ticker"], "weight": row["weight"]}
            for _, row in day_df.iterrows()
        ])

        index_value = (day_df["close_price"] * day_df["weight"]).sum()
        index_perf.append({"trade_date": date, "index_value": index_value})

    # Compose DataFrames
    compositions_df = pd.DataFrame(compositions)
    perf_df = pd.DataFrame(index_perf).sort_values("trade_date")
    perf_df["daily_return"] = perf_df["index_value"].pct_change().fillna(0)
    perf_df["cumulative_return"] = (1 + perf_df["daily_return"]).cumprod() - 1
    perf_df = perf_df.rename(columns={"trade_date": "index_date"})

    # Register DataFrames as DuckDB virtual tables
    print("perf_df columns:", perf_df.columns)

    con.register("comp_view", compositions_df)
    con.register("perf_view", perf_df)

    # Clear old services
    con.execute("DELETE FROM index_composition WHERE date BETWEEN ? AND ?", [start_date, end_date])
    con.execute("DELETE FROM index_performance WHERE index_date BETWEEN ? AND ?", [start_date, end_date])

    # Insert new services
    con.execute("INSERT INTO index_composition SELECT * FROM comp_view")
    con.execute("INSERT INTO index_performance SELECT index_date, daily_return, cumulative_return FROM perf_view")

    con.close()

    # Cache results
    r.set(f"index_perf:{start_date}:{end_date}", perf_df.to_json(orient="records"))

    # Cache daily compositions
    for day in compositions_df["trade_date"].unique():
        day_df = compositions_df[compositions_df["trade_date"] == day][["ticker", "weight"]]
        r.set(f"composition:{day}", day_df.to_json(orient="records"))

    return {"message": f"Index built for {start_date} to {end_date}"}