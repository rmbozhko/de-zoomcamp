from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
# from prefect_gcp.bigquery import BigQueryConnector
from prefect_gcp.bigquery import BigQueryWarehouse
from pandas_gbq import to_gbq

@task(log_prints=True, retries=3, retry_delay_seconds=10)
def extract_from_gcs(dataset_file: str) -> pd.DataFrame:
    """
    Extract data from Google Cloud Storage and return it as a pandas DataFrame.
    """
    
    gcs_bucket = GcsBucket.load("gcs-bucket")
    path = Path(f"data/{dataset_file}.parquet")
    
    gcs_bucket.download_object_to_path(from_path=path.name, to_path=path)
    df = pd.read_parquet(path)
    
    return df


@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the DataFrame by cleaning and converting datetime columns.
    """
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df['passenger_count'].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    print(f"Number of rows in DataFrame: {len(df)}")
    
    return df


@task(log_prints=True, retries=3, retry_delay_seconds=10)
def write_bq(df: pd.DataFrame, dataset_file: str) -> None:
    """
    Load the DataFrame into BigQuery.
    """

    credentials_block = GcpCredentials.load("zoom-gcp-creds")
    
    table_name = f"demo_dataset.hw_q3_table"

    to_gbq(df, table_name, project_id=credentials_block.project, if_exists='append', credentials=credentials_block.get_credentials_from_service_account())
    
    return None

@flow(name="etl_gcs_to_bq", log_prints=True)
def etl_gcs_to_bq(month: int = 1, year: int = 2021, color: str = "yellow"):
    """
    Main ETL flow to extract data from Google Cloud Storage, transform it, and load it into BigQuery.
    """
    dataset_file = f"{color}_tripdata_{year}-{month:02d}"

    df = extract_from_gcs(dataset_file)
    df = transform(df)
    write_bq(df, dataset_file)


@flow(name="parent_etl_gcs_to_bq", log_prints=True)
def parent_etl_gcs_to_bq(
    months: list[int] = [1, 2, 3], year: int = 2021, color: str = "yellow"
):
    """
    Parent ETL flow to run the ETL process for multiple months.
    """
    for month in months:
        etl_gcs_to_bq(month, year, color)


if __name__ == "__main__":
    parent_etl_gcs_to_bq(
        months=[1, 2, 3], year=2021, color="yellow"
    )