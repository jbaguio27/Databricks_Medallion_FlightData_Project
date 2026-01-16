import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = 'stg_flights'
)

def stg_flights():
    df = spark.readStream.format('delta')\
            .load('/Volumes/flight_project/bronze/bronzevolume/flights/data/')
    return df

@dlt.view(
    name = 'trans_flights'
)
def trans_flights():
    df = spark.readStream.table('stg_flights')
    df = df.withColumn('flight_date', to_date(col('flight_date')))\
        .withColumn('modified_date', current_timestamp())\
        .drop('_rescued_data')
    return df

dlt.create_streaming_table(
    name = 'silver_flights'
)

dlt.create_auto_cdc_flow(
  target = "silver_flights",
  source = "trans_flights",
  keys = ['flight_id'],
  sequence_by = "modified_date",
  stored_as_scd_type = 1
)

