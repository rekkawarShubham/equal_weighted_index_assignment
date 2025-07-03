from fastapi import FastAPI
from app.api import index_builder,index_retrieval,index_export_data

app = FastAPI(title="Equal Weighted Index API")
app.include_router(index_builder.router)
app.include_router(index_retrieval.router)
app.include_router(index_export_data.router)


@app.get("/")
def read_root():
    return {"message": "Welcome to the Equal Weighted Index API"}
