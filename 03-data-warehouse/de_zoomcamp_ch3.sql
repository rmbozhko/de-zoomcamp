-- Create a non-partitioned table from external table
CREATE OR REPLACE TABLE `turnkey-banner-469014-v5.demo_dataset.yellow_tripdata_bq_non_partitioned` AS
SELECT * FROM `turnkey-banner-469014-v5.demo_dataset.external_table_yellow_tripdata_bq`;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `turnkey-banner-469014-v5.demo_dataset.yellow_tripdata_bq_partitioned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `turnkey-banner-469014-v5.demo_dataset.external_table_yellow_tripdata_bq`;

-- Impact of partition
-- This query will process 418.48 MB when run.
SELECT DISTINCT(VendorID)
FROM `turnkey-banner-469014-v5.demo_dataset.yellow_tripdata_bq_non_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-06-30';

-- This query will process 8 MB when run.
SELECT DISTINCT(VendorID)
FROM `turnkey-banner-469014-v5.demo_dataset.yellow_tripdata_bq_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-06-30';

-- Take a look at the partitions
SELECT table_name, partition_id, total_rows
FROM `demo_dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_bq_partitioned'
ORDER BY total_rows DESC;

-- Create a partitioned and cluster table from external table
CREATE OR REPLACE TABLE `turnkey-banner-469014-v5.demo_dataset.yellow_tripdata_bq_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY tpep_pickup_datetime, VendorID AS
SELECT * FROM `turnkey-banner-469014-v5.demo_dataset.external_table_yellow_tripdata_bq`;

CREATE OR REPLACE TABLE `turnkey-banner-469014-v5.demo_dataset.yellow_tripdata_bq_partitioned_clustered`
CLUSTER BY tpep_pickup_datetime, VendorID AS
SELECT * FROM `turnkey-banner-469014-v5.demo_dataset.external_table_yellow_tripdata_bq`;

DROP TABLE `turnkey-banner-469014-v5.demo_dataset.yellow_tripdata_bq_partitioned_clustered`;

-- This query will process 123 MB when run.
SELECT count(*)
FROM `turnkey-banner-469014-v5.demo_dataset.yellow_tripdata_bq_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-12-31'
AND VendorID=1;

-- This query will process 120 MB when run.
SELECT count(*)
FROM `turnkey-banner-469014-v5.demo_dataset.yellow_tripdata_bq_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-12-31'
AND VendorID=2;