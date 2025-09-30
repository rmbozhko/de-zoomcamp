CREATE OR REPLACE EXTERNAL TABLE `turnkey-banner-469014-v5.demo_dataset.external_table_green_tripdata_2022`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://turnkey-banner-469014-v5-terra-bucket/raw/green/green_tripdata_2022_*.parquet']
);

CREATE OR REPLACE TABLE `demo_dataset.green_tripdata_2022` AS
SELECT * FROM `demo_dataset.external_table_green_tripdata_2022`;

SELECT DISTINCT PULocationID FROM `demo_dataset.external_table_green_tripdata_2022`;
SELECT DISTINCT PULocationID FROM `demo_dataset.green_tripdata_2022`;

SELECT COUNT(*) FROM `demo_dataset.green_tripdata_2022` WHERE fare_amount = 0;

CREATE OR REPLACE TABLE `demo_dataset.green_tripdata_2022_q5`
PARTITION BY DATE (lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM `demo_dataset.green_tripdata_2022`;


SELECT DISTINCT PULocationID FROM `demo_dataset.green_tripdata_2022` WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
SELECT DISTINCT PULocationID FROM `demo_dataset.green_tripdata_2022_q5` WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';

