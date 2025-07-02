# API
# Question: How do you read data from the CoinCap API given below and write the data to a DuckDB database?

# database table

#     """
# CREATE TABLE IF NOT EXISTS Exchanges (
#     id TEXT,
#     name TEXT,
#     rank INTEGER,
#     percentTotalVolume FLOAT,
#     volumeUsd FLOAT,
#     tradingPairs TEXT,
#     socket BOOLEAN,
#     exchangeUrl TEXT,
#     updated BIGINT 
# )
# """

# URL: "https://api.coincap.io/v2/exchanges"
# Hint: use requests library

import requests

from dotenv import load_dotenv
import os
import duckdb
load_dotenv()

token = os.getenv("COINCAP_API_KEY")

headers = {
    "Authorization": f"Bearer {token}"
}
# Define the API endpoint
url = "https://rest.coincap.io/v3/exchanges"

# Fetch data from the CoinCap API

response = requests.get(url, headers=headers)
data = response.json()
print(response.json())

# Connect to the DuckDB database
connDuckdb = duckdb.connect("duckdb.db")
# Insert data into the DuckDB Exchanges table
for exchange in data['data']:
    exchangeId = exchange.get('exchangeId')
    name = exchange.get('name')
    rank = int(exchange.get('rank'))
    tempPercentTotalVolume = exchange.get('percentTotalVolume')
    percentTotalVolume = float(tempPercentTotalVolume) if tempPercentTotalVolume is not None else 0.0
    tempVolumeUsd = exchange.get('volumeUsd')
    volumeUsd = float(tempVolumeUsd) if tempVolumeUsd is not None else 0.0
    tradingPairs = exchange.get('tradingPairs')
    socket = bool(exchange.get('socket'))
    exchangeUrl = exchange.get('exchangeUrl')
    updated = int(exchange.get('updated'))
    
    connDuckdb.execute(
     """
        INSERT INTO Exchanges (
            id, name, rank, percentTotalVolume,
            volumeUsd, tradingPairs, socket,
            exchangeUrl, updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            exchangeId,
            name,
            rank,
            percentTotalVolume,
            volumeUsd,
            tradingPairs,
            socket,
            exchangeUrl,
            updated
        )
    )
# Prepare data for insertion

# Hint: Ensure that the data types of the data to be inserted is compatible with DuckDBs data column types in ./setup_db.py