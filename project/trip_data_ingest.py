import pandas as pd
import argparse
from sqlalchemy import create_engine
import os
from time import time

def wget(url):
    filename = url.split('/')[-1]
    os.system(f"curl {url} -o {filename}")
    print(f"{filename} DOWNLOADED SUCESSFULLY!")
    return filename

def connect(engine):
    with engine.connect() as connection:
        print("CONNECTION SUCCESSFUL YES!")

def mkschema(filename, psqlengine, db_tablename):
    try:
        data = pd.read_csv(filename, nrows=2)
        for c in data.columns:
            if 'date' in c:
                data[c] = pd.to_datetime(data[c])
        data.head(0).to_sql(name=db_tablename, con=psqlengine, if_exists='replace')
        print("SCHEMA CREATED!!!")
    except Exception as e:
        print(f"Failed to create schema: {e}")

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    db_tablename = params.db_tablename
    url = params.url

    filename = wget(url)

    engaddress = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    psqlengine = create_engine(engaddress)

    connect(psqlengine)
    mkschema(filename, psqlengine, db_tablename)

    chunk_size = 20000
    df_iter = pd.read_csv(filename, iterator=True, chunksize=chunk_size, low_memory=False)
    nrows = len(pd.read_csv(filename, usecols=[1], low_memory=False))
    nchunk = nrows // chunk_size + 1

    for i, df in enumerate(df_iter, start=1):
        tstart = time()

        df.to_sql(name=db_tablename, con=psqlengine, if_exists='append')

        tend = time()

        print(f"Inserted chunk {i} of {nchunk}, took {tend - tstart:.3} secs")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='User name for Postgres')
    parser.add_argument('--password', required=True, help='Password for Postgres')  # Corrected typo here
    parser.add_argument('--host', required=True, help='Host for Postgres')
    parser.add_argument('--port', required=True, help='Port for Postgres')
    parser.add_argument('--db', required=True, help='Database name for Postgres')
    parser.add_argument('--db_tablename', required=True, help='Name of the table where results will be written')
    parser.add_argument('--url', required=True, help='URL of the CSV file')

    args = parser.parse_args()

    main(args)