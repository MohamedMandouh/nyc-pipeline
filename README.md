Project for Data Engineering ZoomCamp

Terraform:
  Step 1: Create the required gcp infrastructure using Terraform.

Airflow:
  Step 2: Download archive data of nyc-yellow-taxi trip data.
  Step 3: Load the raw files into gcp bucket.
  PySpark:
    Step 4: Clean the raw data and enrich it with zones lookup table.
    Step 5: Write the result into a gcp bucket.
  Step 6: Write the result into a bigquery table
  
Kafka:
  Step 7: Produce a stream of new trip data (1-minute mini-batch)
  Step 8: Join the stream with zones lookup data using Pandas.
  Step 9: Write the result into bigquery using the streaming API.
