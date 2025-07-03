from fastapi import APIRouter, Query
from fastapi.responses import FileResponse
import duckdb
import pandas as pd
from tempfile import NamedTemporaryFile
from datetime import datetime

router = APIRouter()
DB_PATH = "services/services/index_data.duckdb"

@router.post("/export-data")
def export_data(start_date: str = Query(...), end_date: str = Query(...)):
    con = duckdb.connect(DB_PATH)

    # Get Index Performance services from index_performance table
    perf_df = con.execute(f"""
        SELECT * FROM index_performance
        WHERE index_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY index_date
    """).fetchdf()

    # Get Index Compositions services from index_composition table
    comp_df = con.execute(f"""
        SELECT * FROM index_composition
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date, ticker
    """).fetchdf()

    # Get Composition Changes services by grouping ticker per date
    raw_comp = comp_df.groupby("date")["ticker"].apply(set).sort_index()
    prev_day = None
    changes = []

    for date, tickers in raw_comp.items():
        if prev_day is None:
            prev_day = tickers
            continue

        entered = sorted(list(tickers - prev_day))
        exited = sorted(list(prev_day - tickers))

        if entered or exited:
            changes.append({
                "date": date,
                "entered": ", ".join(entered),
                "exited": ", ".join(exited)
            })

        prev_day = tickers

    changes_df = pd.DataFrame(changes)

    # need to convert date into string date type
    perf_df["index_date"] = perf_df["index_date"].astype(str)
    comp_df["date"] = comp_df["date"].astype(str)
    changes_df["date"] = changes_df["date"].astype(str)

    #  Writing all to a temporary Excel file
    with NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        with pd.ExcelWriter(tmp.name, engine="openpyxl") as writer:
            perf_df.to_excel(writer, sheet_name="Performance", index=False)
            comp_df.to_excel(writer, sheet_name="Compositions", index=False)
            changes_df.to_excel(writer, sheet_name="Composition Changes", index=False)

        file_path = tmp.name

    today_date = datetime.now().strftime("%Y%m%d")
    filename = f"index_export_{start_date.replace('-', '')}_{end_date.replace('-', '')}.xlsx"

    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )