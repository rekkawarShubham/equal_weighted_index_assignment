from fastapi import APIRouter, Query
from redis import Redis
import duckdb
import pandas as pd
import os
from dotenv import load_dotenv
import json

load_dotenv()

router = APIRouter()
DB_PATH = "data/index_data.duckdb"
r = Redis(host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", 6379)))

@router.get("/index-performance")
def get_index_performance(start_date: str = Query(...), end_date: str = Query(...)):
    cache_key = f"index_perf:{start_date}:{end_date}"
    cached = r.get(cache_key)

    if cached:
        return {
            "source": "redis",
            "services": pd.read_json(cached.decode("utf-8")).to_dict(orient="records")
        }

    con = duckdb.connect(DB_PATH)
    df = con.execute(f"""
        SELECT * FROM index_performance
        WHERE index_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY index_date
    """).fetchdf()

    if df.empty:
        return {"message": "No index services found for given date range"}

    r.set(cache_key, df.to_json(orient="records"), ex=86400)
    return {"source": "db", "services": df.to_dict(orient="records")}

@router.get("/index-composition")
def get_index_composition(date: str = Query(...)):
    cache_key = f"composition:{date}"
    cached = r.get(cache_key)

    if cached:
        return {
            "source": "redis",
            "services": pd.read_json(cached.decode("utf-8")).to_dict(orient="records")
        }

    con = duckdb.connect(DB_PATH)
    df = con.execute(f"""
        SELECT ticker, weight
        FROM index_composition
        WHERE date = '{date}'
        ORDER BY ticker
    """).fetchdf()

    if df.empty:
        return {"message": f"No composition services found for {date}"}

    # Cache and return , also we can set expiry here if needed , ex =3600 (1hour)
    r.set(cache_key, df.to_json(orient="records"),ex=86400)
    return {
        "source": "db",
        "services": df.to_dict(orient="records")
    }

@router.get("/composition-changes")
def get_composition_changes(start_date: str = Query(...), end_date: str = Query(...)):
    # Cached the composition changes for given date range
    cache_key = f"composition_changes:{start_date}:{end_date}"
    cached = r.get(cache_key)
    if cached:
        return {"source": "redis", "changes": json.loads(cached)}

    con = duckdb.connect(DB_PATH)

    # Load all compositions in date range
    df = con.execute(f"""
        SELECT date, ticker
        FROM index_composition
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date, ticker
    """).fetchdf()

    con.close()

    if df.empty:
        return {"message": "No services in range"}

    # Group tickers by date
    grouped = df.groupby("date")["ticker"].apply(set).sort_index()

    changes = []
    prev_date = None
    prev_tickers = set()

    for curr_date, curr_tickers in grouped.items():
        if prev_date is None:
            prev_date = curr_date
            prev_tickers = curr_tickers
            continue

        entered = sorted(list(curr_tickers - prev_tickers))
        exited = sorted(list(prev_tickers - curr_tickers))

        if entered or exited:
            changes.append({
                "date": str(curr_date),
                "entered": entered,
                "exited": exited
            })

        prev_tickers = curr_tickers

    # adding ex=86400 avoids stale data in cache
    r.set(cache_key, json.dumps(changes),ex=86400)
    return {"source": "db" , "changes": changes}