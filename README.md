Docker Compose creates our containers and Cassandra database.

Using Docker Compose and python scrips, the project mocks a running gRPC server and Cassandra database to read and process a stream of weather data.
The gRPC server awaits communication till a client calls. When a client calls to send a data stream it first reads raw csv and/or parquet data and efficiently processes it Apache Spark.
When ir reaches gRPC server it manages into our Cassandra schema depending on the client server input.
