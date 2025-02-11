#!/usr/bin/env python
# coding: utf-8

import argparse

import os
import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine
from time import time
import urllib.request


def main(params):
    user = urllib.parse.quote_plus(params.user)
    password = urllib.parse.quote_plus(params.password)
    host = urllib.parse.quote_plus(params.host)
    port = params.port
    db = urllib.parse.quote_plus(params.db)
    table_name = params.table_name 
    url = params.url


    file_name = 'output.parquet'

    #os.system(f"wget{url} -0 {file_name}")


    # Download the PARQUET file using urllib for Windows compatibility
    print("Downloading the file...")

    # Download the file using urllib instead of wget
    urllib.request.urlretrieve(url, file_name)
    print("Download complete.")


    # download the PARQUET file
    #connection_string = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{db}?client_encoding=utf8'
    engine = create_engine(connection_string)


    df = pd.read_parquet(file_name, engine="pyarrow")
    #df = next(df)

    # Ensure datetime columns are properly formatted
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Insert DataFrame into the database
    print("Inserting data...")
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print("Data ingestion complete.")


    while True:
        t_start = time()
        df = next(df)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end= time()

        print('inserted another chuck, took %.3f second' % (t_end - t_start))



if __name__ == '__main__':
    parser= argparse.ArgumentParser(description= 'Ingest PARQUET file to Postgres')

    #User, password, host, port, database name, table name, url of the PARQUET
    parser.add_argument('--user', help='user name for postgres')  
    parser.add_argument('--password', help='password name for postgres') 
    parser.add_argument('--host', help='host name for postgres')  
    parser.add_argument('--port', help='port name for postgres')  
    parser.add_argument('--db', help='database name for postgres') 
    parser.add_argument('--table_name', help='name of the table where we will writethe results to') 
    parser.add_argument('--url', help='url of the PARQUET file') 

    args = parser.parse_args()

    main(args)







