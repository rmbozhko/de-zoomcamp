from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True, retries=3, retry_delay_seconds=10)
def fetch(url: str) -> pd.DataFrame:
    """
    Fetch data from a given URL and return it as a pandas DataFrame.
    """
    df = pd.read_csv(url)

    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the DataFrame by removing rows with zero passenger count.
    """
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """
    Write the DataFrame to a local CSV file.
    """
    path = Path(f"data/{dataset_file}.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression='gzip')
    
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """
    Write the local file to Google Cloud Storage.
    """
    gcs_bucket = GcsBucket.load("gcs-bucket")
    gcs_bucket.upload_from_path(from_path=path, to_path=path.name)
    print(f"File {path.name} uploaded to GCS bucket {gcs_bucket.bucket}")
    
    return None

@flow(name="etl_web_to_gcs", log_prints=True)
def etl_web_to_gcs(color: str, year: int, month: int):
    """
    Main ETL flow to extract data from a web source, transform it, and load it into Google Cloud Storage.
    """
    dataset_file = f"{color}_tripdata_{year}-{month:02d}"
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df = clean(df)
    print(df.head(n=10))
    path = write_local(df, dataset_file)
    write_gcs(path)
    print(f"Data written to {path}")


if __name__ == "__main__":
    etl_web_to_gcs(
        color="yellow",
        year=2021,
        month=5,
    )