# Equal-Weighted Stock Index Backend

This backend service dynamically constructs, tracks, and exports an equal-weighted stock index of the top 100 US stocks by daily market capitalization.

---

## Features Implemented

- Build equal-weighted  index for any date range
- Retrieve index performance, compositions, and changes
- Export all data to Excel
- Redis caching for fast API responses
- Fully Dockerized setup

---

## Tech Stack Used

- Python 3.12
- FastAPI
- DuckDB
- Redis
- Pandas
- Docker & Docker Compose

---

## Setup Instructions

### Docker
    1. To run the app : docker-compose up --build
    2. To terminate : docker-compose down

### Local Command to run the app

    python -m venv <env-name>
    source <env-name>/bin/activate
    pip install -r requirements.txt
    uvicorn app.main:app --reload

### Access APIs once Docker is up

Access API at: http://localhost:8000/docs

### Daily data ingestion job

    RUN : python data/data_ingestion_job.py

## Build Index
    POST /build-index?start_date=2024-05-01&end_date=2024-06-01

## API Usage
    API : /index-performance
    GET /index-performance?start_date=2024-05-01&end_date=2024-06-01
    
    API : /index-composition
    GET /index-composition?date=2024-05-15
    
    API : /composition-changes
    GET /composition-changes?start_date=2024-05-01&end_date=2024-06-01
    
    API : /export-data
    POST /export-data?start_date=2024-05-01&end_date=2024-06-01
    Returns .xlsx file with:
