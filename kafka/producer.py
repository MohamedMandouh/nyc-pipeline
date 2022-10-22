
import pandas as pd
from datetime import datetime, timedelta
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

def get_kafka_producer():

    key_schema = avro.load("key_schema.avsc")
    value_schema = avro.load("value_schema.avsc")

    producer_config = {
          "bootstrap.servers": "172.18.0.1:9092",
          "schema.registry.url": "http://172.18.0.1:8081",
          "acks": "1"
      }
    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)
  
    return producer

def write_to_kafka(producer, records, topic):

    for record in records:
        key= {'pickup_datetime' : record['pickup_datetime'] }
        value= record
        producer.produce(topic= topic, key=key, value=value)

    producer.flush()
    if records:
        print('flushed ' + records[0]['pickup_datetime'])
    else: print('Empty')
    
def prepare_yellow_trips():
    df_yellow= pd \
    .read_parquet('http://localhost:8000/yellow_tripdata_2021_04.parquet')

    df_yellow= df_yellow.rename(columns= {
    'tpep_pickup_datetime':'pickup_datetime',
    'tpep_dropoff_datetime': 'dropoff_datetime',
    'PULocationID': 'start_location_id',
    'DOLocationID': 'end_location_id'
    })

    df_yellow= df_yellow \
    .drop(columns= ['store_and_fwd_flag', 'VendorID', 'RatecodeID']) \
    .fillna({'airport_fee' : 0.0, 'congestion_surcharge' : 0.0, 'passenger_count' : 0.0})

    df_yellow = df_yellow.astype({'passenger_count': 'int64'})
    
    return df_yellow

def add_delta(start, minutes) : 
    return start.__add__(timedelta(minutes= minutes))

def produce_stream(producer, topic, start, end) :
 
    df_yellow = prepare_yellow_trips()

    interval_end= add_delta(start, 1)

    while (start < end) :

        df_new= df_yellow[(df_yellow['pickup_datetime'] > start) \
            & (df_yellow['pickup_datetime'] < interval_end)]

        #timestamp type isn't supported for avro schema
        df_new = df_new.astype({'pickup_datetime': 'string',
        'dropoff_datetime': 'string'})

        records= df_new.to_dict('records')
        
        write_to_kafka(producer, records, topic)

        start = add_delta(start, 1)
        interval_end= add_delta(interval_end, 1)


if __name__ == "__main__" :
    start= datetime(year=2021, month=4, day= 1, hour=0, minute=0, second=0)
    end= datetime(year=2021, month=5, day= 1, hour=0, minute=0, second=0)
    producer= get_kafka_producer()
    produce_stream(producer, "yellow_stream_v1", start, end)

  


