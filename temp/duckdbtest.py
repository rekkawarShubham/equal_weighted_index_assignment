import duckdb
con = duckdb.connect("/Users/shubhamrekkawar/stock_market_analysis/data/data/index_data.duckdb")
print(con.execute("SELECT * FROM daily_stock_data LIMIT 5").fetchdf())
print(con.execute("SELECT * FROM index_composition LIMIT 5").fetchdf())
print(con.execute("SELECT * FROM index_performance LIMIT 5").fetchdf())
con.close()
