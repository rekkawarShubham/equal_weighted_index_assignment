from fastapi import APIRouter, Query
from redis import Redis
import duckdb
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()
DB_PATH = "data/data/index_data.duckdb"
r = Redis(host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", 6379)))

@router.get("/index-performance")
def get_index_performance(start_date: str = Query(...), end_date: str = Query(...)):
    cache_key = f"index_perf:{start_date}:{end_date}"
    cached = r.get(cache_key)

    if cached:
        return {
            "source": "redis",
            "data": pd.read_json(cached.decode("utf-8")).to_dict(orient="records")
        }

    con = duckdb.connect(DB_PATH)
    df = con.execute(f"""
        SELECT * FROM index_performance
        WHERE index_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY index_date
    """).fetchdf()

    if df.empty:
        return {"message": "No index data found for given date range"}

    r.set(cache_key, df.to_json(orient="records"))
    return {"source": "db", "data": df.to_dict(orient="records")}

@router.get("/index-composition")
def get_index_composition(date: str = Query(...)):
    cache_key = f"composition:{date}"
    cached = r.get(cache_key)

    if cached:
        return {
            "source": "redis",
            "data": pd.read_json(cached.decode("utf-8")).to_dict(orient="records")
        }

    con = duckdb.connect(DB_PATH)
    df = con.execute(f"""
        SELECT ticker, weight
        FROM index_composition
        WHERE date = '{date}'
        ORDER BY ticker
    """).fetchdf()

    if df.empty:
        return {"message": f"No composition data found for {date}"}

    # Cache and return
    r.set(cache_key, df.to_json(orient="records"))
    return {
        "source": "db",
        "data": df.to_dict(orient="records")
    }
