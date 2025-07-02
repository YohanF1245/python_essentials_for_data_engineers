import boto3
import botocore
import gzip
import csv
import duckdb

connDuckdb = duckdb.connect('duckdb.db')

bucket_name = "noaa-ghcn-pds"
file_key = "csv.gz/by_station/ASN00002022.csv.gz"

s3 = boto3.client('s3', config=botocore.config.Config(signature_version=botocore.UNSIGNED))

response = s3.get_object(Bucket=bucket_name, Key=file_key)
content = response['Body'].read()
unzipped_content = gzip.decompress(content).decode('utf-8')

# structure duckdb weather
#  id TEXT,
#     date TEXT,
#     element TEXT,
#     value INTEGER,
#     m_flag TEXT,
#     q_flag TEXT,
#     s_flag TEXT,
#     obs_time TEXT

with open('weather_data.csv', 'wt') as f:
    f.write(unzipped_content)

with open('weather_data.csv', newline='') as f2:
    weather_data = csv.reader(f2, delimiter = ' ', quotechar = '|')
    for row in weather_data:
        cols = row[0].split(',')
        print(cols)
        connDuckdb.execute(f"INSERT INTO WeatherData VALUES ('{cols[0]}', '{cols[1]}', '{cols[2]}', '{cols[3]}', '{cols[4]}', '{cols[5]}', '{cols[6]}', '{cols[7]}')")

connDuckdb.commit()
connDuckdb.close()

