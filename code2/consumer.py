from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Kafka_to_Cassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "ip-172-31-6-42.eu-west-2.compute.internal:9092,ip-172-31-3-85.eu-west-2.compute.internal:9092") \
    .option("subscribe", "test_shopee") \
    .load()

# Schema matching Cassandra table
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("order_item_id", IntegerType()),
    StructField("commission_amount", DoubleType()),
    StructField("customer_id", StringType()),
    StructField("discount_percent", DoubleType()),
    StructField("estimated_delivery_end", StringType()),
    StructField("estimated_delivery_start", StringType()),
    StructField("is_campaign", BooleanType()),
    StructField("item_status", StringType()),
    StructField("line_total", DoubleType()),
    StructField("maintenance_amount", DoubleType()),
    StructField("order_date", StringType()),  # convert later
    StructField("product_campaign_id", DoubleType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("shipping_fee_item", DoubleType()),
    StructField("unit_price", DoubleType()),
    StructField("unit_price_after_discount", DoubleType())
])

# Parse JSON from Kafka
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
print(json_df)
# Convert order_date to proper date type
final_df = json_df.withColumn(
    "order_date",
    to_date(col("order_date"), "yyyy-MM-dd")
)
'''
# Write to Cassandra
query = final_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "shopee_ord") \
    .option("table", "shopee_order_items") \
    .option("checkpointLocation", "/tmp/checkpoint/cassandra") \
    .outputMode("append") \
    .start()'''
    
  

  

query = final_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/rutvik_proj/kafka/shopee_prd") \
    .option("checkpointLocation", "hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/rutvik_proj/checkpoints/shopee_prd2") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .start()

query.awaitTermination()



#.partitionBy("order_date") \