import duckdb
import pandas as pd
con = duckdb.connect("/Users/shubhamrekkawar/stock_market_analysis/data/data/index_data.duckdb")
print(con.execute("SELECT * FROM daily_stock_data LIMIT 5").fetchdf())
# # print(con.execute("SELECT * FROM index_composition").fetchdf())
# # print(con.execute("SELECT * FROM index_performance LIMIT 5").fetchdf())
#
# # df = con.execute("""
# #     SELECT date, COUNT(*) AS ticker_count
# #     FROM index_composition
# #     GROUP BY date
# #     ORDER BY date
# # """).fetchdf()
# # print(df)
#
# TICKERS_TO_INGEST = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()
# print((TICKERS_TO_INGEST[:150]))

con.close()
