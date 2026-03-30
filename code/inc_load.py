from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from logger_config import get_logger
logger = get_logger(__name__)




try:
  logger.info("Spark Session Start....")
  spark = SparkSession.builder.appName("rutu_full_lode").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  jdbc_url = "jdbc:postgresql://13.42.152.118:5432/testdb"

  properties = {
  "user": "admin",
  "password": "admin123",
  "driver": "org.postgresql.Driver"
  }



  def new_rec(df1,df2,col=""):
    result_df = df2.join(df1, on=col, how="left_anti")
    return result_df

    #creating data frame

  logger.info("Reading New Data....")
  df_cus_new = spark.read.jdbc(url=jdbc_url,table="rutvik.shopee_customers_th",properties=properties)
  df_ord_itm_new =spark.read.parquet("/tmp/rutvik_proj/kafka/shopee_prd/part*")
  df_prd_new=spark.read.csv('file:///home/ec2-user/250226batch/rutvik/project/data/shopee_products_thailand.csv',header=True,inferSchema=True)
  df_seller_new=spark.read.csv('file:///home/ec2-user/250226batch/rutvik/project/data/shopee_sellers_thailand.csv',header=True,inferSchema=True)


  #getting old data

  logger.info("Reading Old Data....")
  df_cus_old = spark.read.parquet('/tmp/rutvik_proj/bronze/shopee_cus')
  df_ord_itm_old = spark.read.parquet('/tmp/rutvik_proj/bronze/shopee_ord_itm')
  df_prd_old=spark.read.parquet('/tmp/rutvik_proj/bronze/shopee_prd')
  df_seller_old=spark.read.parquet('/tmp/rutvik_proj/bronze/shopee_seller')

  logger.info("Checking For New Data And Writing....")
  df_cus_inc=new_rec(df_cus_old,df_cus_new,col='customer_id')
  if df_cus_inc>0:
    df_cus_inc.write.mode("append").parquet("/tmp/rutvik_proj/bronze/shopee_cus")

  df_prd_inc=new_rec(df_prd_old,df_prd_new,col='product_id')
  if df_prd_inc>0:
    df_prd_inc.write.mode("append").parquet("/tmp/rutvik_proj/bronze/sshopee_prd")

  df_seller_inc=new_rec(df_seller_old,df_seller_new,col='seller_id')
  if df_seller_inc>0:
    df_seller_inc.write.mode("append").parquet("/tmp/rutvik_proj/bronze/shopee_seller")


  max_date = df_ord_itm_old.agg(max("order_date").alias("max_date")).collect()[0]["max_date"]
  df_ord_itm_inc = df_ord_itm_new.filter(col("order_date") > max_date)
  if df_ord_itm_inc.count() > 0:
    df_ord_itm_inc.write.mode("append").parquet("/tmp/rutvik_proj/bronze/shopee_ord_itm")
    print('Adding Data.....')








