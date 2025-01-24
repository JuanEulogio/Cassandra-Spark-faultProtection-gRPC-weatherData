import grpc
import station_pb2
import station_pb2_grpc
import zipfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date

SERVER = "localhost:5440"

def run():
    spark = SparkSession.builder.appName("p6-loader").getOrCreate()
    data = spark.read.parquet("weather.parquet").collect()

    # Connect to the gRPC server
    with grpc.insecure_channel('localhost:5440') as channel:
        stub = station_pb2_grpc.StationStub(channel)
        for row in data:
            station_id = row["station"]
            date = row["date"]
            tmin = row["tmin_value"] if row["tmin_value"] is not None else 0
            tmax = row["tmax_value"] if row["tmax_value"] is not None else 0
            
            # Make the gRPC call
            response = stub.RecordTemps(station_pb2.RecordTempsRequest(
                station=station_id,
                date=date,
                tmin=int(tmin),
                tmax=int(tmax)
            ))
            
            # Print the response for each record
            if response.error:
                print(f"Error inserting {station_id} on {date}: {response.error}")
            else:
                print(f"Inserted {station_id} on {date} with tmin={tmin} and tmax={tmax}")

if __name__ == '__main__':
    run()
