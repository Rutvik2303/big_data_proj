from pyspark.sql import SparkSession
from logger_config import get_logger

logger = get_logger(__name__)
logger.info("Pipeline started...")

try:

  logger.info("Spark Session Start....")
  spark = SparkSession.builder.appName("rutu_full_lode").getOrCreate()
  spark.sparkContext.setLogLevel("Error")
  jdbc_url = "jdbc:postgresql://13.42.152.118:5432/testdb"

  properties = {
  "user": "admin",
  "password": "admin123",
  "driver": "org.postgresql.Driver"
  }

  #creating data frame
  logger.info("Reading Data from DB and FS....")
  df_cus = spark.read.jdbc(url=jdbc_url,table="rutvik.shopee_customers_th",properties=properties)
  df_ord_itm =spark.read.jdbc(url=jdbc_url,table="rutvik.shopee_order_items",properties=properties)
  df_prd=spark.read.csv('file:///home/ec2-user/250226batch/rutvik/project/data/shopee_products_thailand.csv',header=True,inferSchema=True)
  df_seller=spark.read.csv('file:///home/ec2-user/250226batch/rutvik/project/data/shopee_sellers_thailand.csv',header=True,inferSchema=True)




  logger.info("Writing Data to Bronze....")
  df_cus.write.mode("overwrite").parquet("hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/rutvik_proj/bronze/shopee_cus")
  df_ord_itm.write.mode("overwrite").parquet("hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/rutvik_proj/bronze/shopee_ord_itm")
  df_seller.write.mode("overwrite").parquet("hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/rutvik_proj/bronze/shopee_seller")
  df_prd.write.mode("overwrite").parquet("hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/rutvik_proj/bronze/shopee_prd")

except Exception as e:
  logger.error("Failed: {}".format(e), exc_info=True)