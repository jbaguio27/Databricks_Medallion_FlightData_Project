import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = 'stg_passengers'
)

def stg_passengers():
    df = spark.readStream.format('delta')\
            .load('/Volumes/flight_project/bronze/bronzevolume/customers/data/')
    return df

@dlt.view(
    name = 'trans_passengers'
)
def trans_passengers():
    df = spark.readStream.table('stg_passengers')
    df = df.withColumn('modified_date', current_timestamp())\
        .drop('_rescued_data')
    return df

dlt.create_streaming_table(
    name = 'silver_passengers'
)

dlt.create_auto_cdc_flow(
  target = "silver_passengers",
  source = "trans_passengers",
  keys = ['passenger_id'],
  sequence_by = "modified_date",
  stored_as_scd_type = 1
)

