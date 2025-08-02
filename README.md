Docker Compose creates our containers and Cassandra database. When we run our gRPC server it awaits communicates to receives a stream of data via our client server.
Our client server efficiently processes data by using Spark to acts as our intermediary data processing. The data consist of csv and parquet variations.
When the client server sends the data, the server manages into our Cassandra schema by logically processing it depending on the client server input. 
