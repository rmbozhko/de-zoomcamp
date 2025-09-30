SELECT passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tolls_amount, tip_amount
FROM `demo_dataset.yellow_tripdata_bq_partitioned` WHERE fare_amount != 0;

CREATE OR REPLACE TABLE `demo_dataset.yellow_tripdata_ml` (
  `passenger_count` INTEGER,
  `trip_distance` FLOAT64,
  `PULocationID` STRING,
  `DOLocationID` STRING,
  `payment_type` STRING,
  `fare_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `tip_amount` FLOAT64,
) AS (
  SELECT passenger_count, trip_distance, CAST(PULocationID AS STRING), CAST(DOLocationID AS STRING), CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount FROM `demo_dataset.yellow_tripdata_bq_partitioned` WHERE fare_amount != 0
);

CREATE OR REPLACE MODEL `demo_dataset.tip_model`
OPTIONS (model_type='linear_reg', input_label_cols=['tip_amount'], DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT * FROM `demo_dataset.yellow_tripdata_ml` WHERE tip_amount IS NOT NULL;

SELECT * FROM ML.FEATURE_INFO(MODEL `demo_dataset.tip_model`);

-- The following data can be used to optimize the model later
SELECT * FROM ML.EVALUATE(MODEL `demo_dataset.tip_model`,
  (SELECT * FROM `demo_dataset.yellow_tripdata_ml` WHERE tip_amount IS NOT NULL)
);

-- Split your dataset into training, validation (combined) and separate test tables
-- So, you wouldn't run testing of model inference capabilities on data used for training and validation
SELECT predicted_tip_amount, tip_amount, (predicted_tip_amount - tip_amount) AS tip_diff
FROM ML.PREDICT(MODEL `turnkey-banner-469014-v5.demo_dataset.tip_model`, (
  SELECT
     *
  FROM `demo_dataset.yellow_tripdata_ml`
));

-- Detect top k features used for prediction
SELECT *
FROM ML.EXPLAIN_PREDICT(MODEL `turnkey-banner-469014-v5.demo_dataset.tip_model`, (
  SELECT
     *
  FROM `demo_dataset.yellow_tripdata_ml`
  WHERE tip_amount IS NOT NULL
), STRUCT(3 as top_k_features));


-- Tune hyperparameters
CREATE OR REPLACE MODEL `demo_dataset.tip_hyperparam_model`
OPTIONS (
  model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT',
  NUM_TRIALS=5,
  MAX_PARALLEL_TRIALS=3,
  L1_REG=hparam_range(0, 20),
  l2_reg=hparam_candidates([0, 0.1, 1, 10])) AS
SELECT * FROM `demo_dataset.yellow_tripdata_ml` WHERE tip_amount IS NOT NULL;