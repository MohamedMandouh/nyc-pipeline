import argparse
from pyspark.sql import SparkSession
import dataproc_job_utils

parser = argparse.ArgumentParser()
parser.add_argument('--yellow', required=True)
parser.add_argument('--zones', required=True)
parser.add_argument('--out', required=True)
parser.add_argument('--bucket', required=True)
args = parser.parse_args()

BUCKET= args.bucket
YELLOW_FILE = BUCKET + '/' + args.yellow
ZONES_FILE= BUCKET + '/' + args.zones
OUTPUT_PATH= args.out

spark = SparkSession.builder \
    .appName('transform') \
    .getOrCreate()

df_yellow_raw = spark.read \
.parquet(YELLOW_FILE)

df_zones_raw = spark.read \
    .option('header', 'true') \
    .csv(str(ZONES_FILE))

df_yellow = dataproc_job_utils.prepare_yellow(df_yellow_raw)
df_zones = dataproc_job_utils.prepare_zones(df_zones_raw)
df_result = dataproc_job_utils.join_yellow_zones(df_yellow, df_zones)

df_result.coalesce(1).write \
    .parquet(f'{BUCKET}/{OUTPUT_PATH}' , mode= 'overwrite')

df_zones.coalesce(1).write \
    .parquet(f'{BUCKET}/new_zones', mode= 'ignore')
