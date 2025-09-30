CREATE OR REPLACE EXTERNAL TABLE `turnkey-banner-469014-v5.demo_dataset.external_table_yellow_tripdata_half_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://turnkey-banner-469014-v5-terra-bucket/raw/yellow/yellow_tripdata_2024_*.parquet']
);

SELECT * FROM `demo_dataset.external_table_yellow_tripdata_half_2024` LIMIT 10;

-- q1
CREATE OR REPLACE TABLE `demo_dataset.yello_tripdata_half_2024` AS
SELECT * FROM `demo_dataset.external_table_yellow_tripdata_half_2024`;

-- q2
SELECT DISTINCT PULocationID FROM `demo_dataset.external_table_yellow_tripdata_half_2024`;
SELECT DISTINCT PULocationID FROM `demo_dataset.yello_tripdata_half_2024`;

-- q3
SELECT PULocationID FROM `demo_dataset.yello_tripdata_half_2024`;
SELECT PULocationID, DOLocationID FROM `demo_dataset.yello_tripdata_half_2024`;

-- q4
SELECT COUNT(*) FROM `demo_dataset.yello_tripdata_half_2024` WHERE fare_amount = 0;
SELECT COUNT(*) FROM `demo_dataset.yello_tripdata_half_2024`;

-- q5
CREATE OR REPLACE TABLE `demo_dataset.yello_tripdata_half_2024_q5`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `demo_dataset.external_table_yellow_tripdata_half_2024`;

-- q6
SELECT DISTINCT VendorID FROM `demo_dataset.yello_tripdata_half_2024` WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
SELECT DISTINCT VendorID FROM `demo_dataset.yello_tripdata_half_2024_q5` WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

