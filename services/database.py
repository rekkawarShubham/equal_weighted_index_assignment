import duckdb
import os

DATABASE_FILE = os.getenv("DUCKDB_PATH", "./data/index_data.duckdb")

def get_db_connection():
    """Establishes and returns a DuckDB connection."""
    os.makedirs(os.path.dirname(DATABASE_FILE), exist_ok=True)
    conn = duckdb.connect(database=DATABASE_FILE, read_only=False)
    return conn

def initialize_db():
    """Creates tables if they don't exist."""
    conn = get_db_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_metadata (
                ticker VARCHAR PRIMARY KEY,
                company_name VARCHAR,
                sector VARCHAR,
                industry VARCHAR,
                exchange VARCHAR
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_stock_data (
                trade_date DATE NOT NULL,
                ticker VARCHAR NOT NULL,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                volume BIGINT,
                market_cap DOUBLE, -- Market cap will be calculated/approximated or fetched
                PRIMARY KEY (trade_date, ticker)
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS index_compositions (
                index_date DATE NOT NULL,
                ticker VARCHAR NOT NULL,
                weight DOUBLE,
                notional_value DOUBLE,
                PRIMARY KEY (index_date, ticker)
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS index_performance (
                index_date DATE NOT NULL PRIMARY KEY,
                daily_return DOUBLE,
                cumulative_return DOUBLE
            );
        """)


        print("DuckDB tables initialized successfully.")
    except Exception as e:
        print(f"Error initializing DuckDB tables: {e}")
    finally:
        conn.close()

def create_index_tables():
    conn = get_db_connection()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_composition (
            date DATE,
            ticker TEXT,
            weight DOUBLE,
            PRIMARY KEY (date, ticker)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_performance (
            date DATE PRIMARY KEY,
            index_value DOUBLE,
            daily_return DOUBLE,
            cumulative_return DOUBLE
        );
    """)

    conn.close()

if __name__ == '__main__':
    # This block allows you to run this file directly to initialize the DB
    initialize_db()
    create_index_tables()