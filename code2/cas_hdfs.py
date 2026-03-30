from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("rutu_full_lode") \
    .config("spark.cassandra.connection.host", "192.168.68.52") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.5.2") \
    .getOrCreate()





 

 
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="users", keyspace="demo_keyspace") \
    .load()
 
df.show(10)




