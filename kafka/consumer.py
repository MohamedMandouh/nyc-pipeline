from confluent_kafka.avro import AvroConsumer
from google.cloud import bigquery
import pandas as pd

def get_kafka_consumer():
    consumer_config = {"bootstrap.servers": "172.21.0.1:9092",
        "schema.registry.url": "http://172.21.0.1:8081",
            "group.id": "zoomcamp.taxi.avro.consumer.1",
            "auto.offset.reset": "earliest" }

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["yellow_stream_v1"])

    return consumer

def read_stream(consumer, batch_size):
    records = []
    for i in range (0, batch_size) :
        try:
            message = consumer.poll(5)
        except Exception as e:
             break
        else:
            if message and isinstance(message.value(), dict): #account for max.poll.interval.ms message
                records.append(message.value())
                consumer.commit()
            else:
                break
    return records

def join_yellow_zones(batch):

    df_yellow =pd.DataFrame.from_records(batch)
    df_zones= pd.read_parquet('http://localhost:8001/zones.parquet')

    df_joined= df_yellow.set_index(['start_location_id']) \
    .join(df_zones.rename(columns={'LocationID' : 'start_location_id'}) \
    .set_index('start_location_id'),
    how= 'inner') \
    .reset_index() \
    .rename(columns= {'Zone' : 'start_zone', 'District' : 'start_district'})

    df_joined= df_joined.set_index(['end_location_id']) \
    .join(df_zones.rename(columns={'LocationID' : 'end_location_id'}) \
    .set_index('end_location_id'),
    how= 'inner') \
    .reset_index() \
    .rename(columns= {'Zone' : 'end_zone', 'District' : 'end_district'})

    #reorder columns to conform to bigquery schema
    cols= df_joined.columns.to_list()
    new_cols= cols[2:6]
    new_cols.append(cols[1]) #start_location_id
    new_cols.append(cols[0]) #end_location_id
    new_cols= new_cols + cols[6:]

    records= df_joined[new_cols].to_dict('records')
    return records

def write_bigquery(records):
    client = bigquery.Client()

    errors = client.insert_rows_json('zoom-camp-me.yellow_taxi.new_yellow_trips', records)
    if errors == []:
        print("New rows have been added." + str(len(records)))
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

if __name__ == '__main__' :
    consumer = get_kafka_consumer()
    while (True) :
        batch= read_stream(consumer, 50)
        if not batch: 
            print('no more records')
            break
        else:
            records = join_yellow_zones(batch)
            write_bigquery(records)


