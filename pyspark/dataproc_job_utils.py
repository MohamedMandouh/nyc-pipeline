from pyspark.sql import types

def prepare_zones(df_zones) :

    df_zones= df_zones \
        .drop('service_zone') \
        .replace(to_replace='Unknown', value=None, subset= 'Borough') \
        .withColumn('LocationID', df_zones.LocationID.cast(types.LongType())) \
        .dropna(subset= 'Borough')
    
    return df_zones

def prepare_yellow(df_yellow) :

    df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime','pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime') \
    .withColumnRenamed('PULocationID', 'start_location_id') \
    .withColumnRenamed('DOLocationID', 'end_location_id')

    df_yellow = df_yellow \
    .withColumn('passenger_count', df_yellow.passenger_count.cast(types.LongType()))


    df_yellow= df_yellow \
    .drop('Store_and_fwd_flag', 'VendorID', 'RateCodeID') \
    .fillna({'airport_fee' : 0.0, 'congestion_surcharge' : 0.0, 'passenger_count' : 0})
    
    return df_yellow

def join_yellow_zones(df_yellow, df_zones) :
    #Join yellow trip data with zones lookup table
    df_joined = df_yellow.join(df_zones, \
        df_yellow.start_location_id == df_zones.LocationID, 'inner' ) \
        .drop('LocationID') \
        .withColumnRenamed('Zone', 'start_zone') \
        .withColumnRenamed('Borough', 'start_district')

    df_joined = df_joined.join(df_zones, \
        df_joined.end_location_id == df_zones.LocationID, 'inner' ) \
        .drop('LocationID') \
        .withColumnRenamed('Zone', 'end_zone') \
        .withColumnRenamed('Borough', 'end_district')
    
    return df_joined
