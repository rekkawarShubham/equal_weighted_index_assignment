from fastapi import FastAPI
from api import index_builder,index_retrieval,index_export_data
from data.data_ingestion_job import run_data_ingestion_with_polars
import schedule
import threading
from contextlib import asynccontextmanager


def run_ingestion():
    print("Running scheduled data ingestion...")
    run_data_ingestion_with_polars()


schedule.every().day.at("05:56").do(run_ingestion)

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     print(" App starting up...")
#     # Here we are passing our scheduler function run_scheduler()
#     thread = threading.Thread(target=run_scheduler, daemon=True)
#     thread.start()
#     yield
#
#     print(" App shutting down...")
app = FastAPI(title="Equal Weighted Index API")

app.include_router(index_builder.router)
app.include_router(index_retrieval.router)
app.include_router(index_export_data.router)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Equal Weighted Index API"}