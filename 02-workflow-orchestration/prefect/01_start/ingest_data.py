import pandas as pd
import sqlalchemy as sqla
from time import time
import requests
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector
import argparse


def import_data(iter, engine):
    t_start = time()
    for i, chunk in enumerate(iter):
        print(f'Processing chunk {i}')
        chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'])
        chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'])
        chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        t_end = time()
        print(f'Inserted chunk {i} in {t_end - t_start:.3f} seconds')


@task(log_prints=True, retries=3, retry_delay_seconds=10,
      cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    if url.endswith('.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    
    # Download the CSV file
    response = requests.get(url, stream=True)
    with open(csv_name, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True, retries=3, retry_delay_seconds=10)
def load_data(table_name: str, df: pd.DataFrame):
    connection_block = SqlAlchemyConnector.load("sqlalchemy-connector")
    with connection_block.get_connection(begin=False) as engine:
       df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
       df.to_sql(name=table_name, con=engine, if_exists='append')


@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@flow(name="Subflow", log_prints=True)
def subflow(table_name: str):
    print(f"Running subflow with table name: {table_name}")


@flow(name="Ingest Flow")
def main_flow(table_name: str):
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-04.csv.gz"

    subflow(table_name)

    raw_data = extract_data(url)              # returns DataFrame
    transformed_data = transform_data(raw_data)
    load_data(table_name, transformed_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest NYC Taxi Data")
    parser.add_argument("--table_name", type=str, required=True,
                        help="Name of the table to ingest data into")
    args = parser.parse_args()
    main_flow(args.table_name)
