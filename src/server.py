import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, expr
from cassandra.query import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra import Unavailable


class StationService(station_pb2_grpc.StationServicer):        
    
    def __init__(self):
        # TODO: create schema for weather data; 
        cluster = Cluster(["p6-db-1", "p6-db-2", "p6-db-3"])
        self.cass = cluster.connect()

        # drop a weather keyspace if it already exists
        self.cass.execute("DROP KEYSPACE IF EXISTS weather")

        # create a weather keyspace with 3x replication
        self.cass.execute("""
            CREATE KEYSPACE weather 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
        """)
    
        # self.cass.set_keyspace('use weather')
        self.cass.execute("use weather")

        # inside weather, create a station_record type containing two ints: tmin and tmax
        self.cass.execute("""
            CREATE TYPE station_record (tmin int, tmax int)
        """)

        # inside weather, create a stations table
            # id is a partition key and corresponds to a station's ID (like 'USC00470273')
            # date is a cluster key, ascending
            # name is a static field (because there is only one name per ID).  Example: 'UW ARBORETUM - MADISON'
            # record is a regular field because there will be many records per station partition.
        self.cass.execute("""
            CREATE TABLE stations (
                id text,
                name text STATIC,
                date date,
                record station_record,
                PRIMARY KEY (id, date)
            ) WITH CLUSTERING ORDER BY (date ASC)
        """)

        # spark setup
        self.spark = SparkSession.builder.appName("p6").getOrCreate()


        # TODO: load station data from ghcnd-stations.txt; 
        spark_stations_df = self.spark.read.text("ghcnd-stations.txt")

        # Extract fields using substring based on offsets
        stations_df = (
            spark_stations_df
                .withColumn("id", expr("substring(value, 1, 11)")) #note 2nd parameter for subtring is the length that we want to take
                .withColumn("state", expr("substring(value, 39, 2)"))
                .withColumn("name", expr("substring(value, 42, 30)"))
        )


        # Filter for Wisconsin stations
        wisconsinStations = stations_df.filter(stations_df.state == "WI").select("id", "name").collect()

        # Insert data into Cassandra
        insertStation = self.cass.prepare(
            """
                INSERT INTO weather.stations (id, name)
                VALUES (?, ?)
            """)

        for row in wisconsinStations:
            self.cass.execute(insertStation, (row.id, row.name))
        
        
        # ============ Server Stated Successfully =============
        print("Server started") # Don't delete this line!


    def StationSchema(self, request, context):
        # execute describe table weather.stations cassandra query
        # print(cass.execute("describe table weather.stations").one().create_statement)
        # extract the create_statement from result
        ret= ""
        error= ""
        try:
            ret= self.cass.execute("describe table weather.stations").one().create_statement
        except Exception as e:
            error = traceback.format_exc()

        return station_pb2.StationSchemaReply(schema= ret, error=error)




    def StationName(self, request, context):
        retName= ""
        error= ""
        try:
            # remember to prepare
            cassNamePrepare= self.cass.prepare(
            """
                SELECT name FROM weather.stations WHERE id = ?
            """)
            res= self.cass.execute(cassNamePrepare, (request.station, )).one()

            if res:
                retName = res.name
            else:
                error= "TODO"

        
        except Exception as e:
            error = str(e)


        return station_pb2.StationNameReply(name= str(retName), error=error)


    def RecordTemps(self, request, context):
        error= ""
        try:
            insertTemps= self.cass.prepare(
                """
                    INSERT INTO weather.stations(id, date, record)
                    VALUES (?, ?, {tmin: ?, tmax: ?})
                """)

            insertTemps.consistency_level = ConsistencyLevel.ONE

            self.cass.execute(insertTemps, (request.station, request.date, request.tmin, request.tmax))

        except Unavailable:
            error = "unavailable"
        except NoHostAvailable:
            error = "unavailable"
        except Exception as e:
            error = str(e)

        return station_pb2.RecordTempsReply(error= error)


    def StationMax(self, request, context):
        error= ""
        try:
            stationMax= self.cass.prepare(
                """
                    SELECT MAX(record.tmax) as max_tmax FROM weather.stations where id= ? 
                """
            )

            stationMax.consistency_level = ConsistencyLevel.THREE


            tmaxTable= self.cass.execute(stationMax, (request.station,)).one()

            return station_pb2.StationMaxReply(tmax= tmaxTable.max_tmax, error=error)

        except Unavailable:
            error = "unavailable"
        except NoHostAvailable:
            error = "unavailable"
        except Exception as e:
            error = str(e)
        return station_pb2.StationMaxReply(tmax= 0, error=error)


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=9),
        options=[("grpc.so_reuseport", 0)],
    )
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port('0.0.0.0:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()


