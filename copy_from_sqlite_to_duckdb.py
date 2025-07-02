import duckdb
print(duckdb.__version__)
import sqlite3

connSqlite = sqlite3.connect("tpch.db")
connDuckdb = duckdb.connect("duckdb.db")

customer_table = connSqlite.execute("SELECT * FROM Customer");

for row in customer_table:
    connDuckdb.execute(f"INSERT INTO Customer VALUES {row}")