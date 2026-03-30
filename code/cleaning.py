from pyspark.sql import SparkSession

from logger_config import get_logger
logger = get_logger(__name__)

logger.info("Spark Session Start....")
spark = SparkSession.builder.appName("rutu_full_lode").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")



#Creating Data Frame

logger.info("Readind Data from Bronze....")
df_cus = spark.read.parquet('/tmp/rutvik_proj/bronze/shopee_cus')
df_ord_itm = spark.read.parquet('/tmp/rutvik_proj/bronze/shopee_ord_itm')
df_prd=spark.read.parquet('/tmp/rutvik_proj/bronze/shopee_prd')
df_seller=spark.read.parquet('/tmp/rutvik_proj/bronze/shopee_seller')


#Cleaning Data From Cust....
logger.info("Cleaning Data....")
df_cus=df_cus.filter(df_cus.customer_id.isNotNull())
df_cus=df_cus.dropDuplicates(["customer_id"])
df_cus=df_cus.drop("province")
df_cus=df_cus.withColumn('dob',df_cus['dob'].cast('date'))
df_cus=df_cus.withColumn('registration_date',df_cus['registration_date'].cast('date'))


#Cleaning Data From Order....
df_ord_itm=df_ord_itm.filter(df_ord_itm.order_id.isNotNull())
df_ord_itm=df_ord_itm.dropDuplicates(["order_item_id"])
df_ord_itm=df_ord_itm.withColumn('estimated_delivery_start',df_ord_itm['estimated_delivery_start'].cast('date'))
df_ord_itm=df_ord_itm.withColumn('estimated_delivery_end',df_ord_itm['estimated_delivery_end'].cast('date'))
df_ord_itm=df_ord_itm.withColumn('order_date',df_ord_itm['order_date'].cast('date'))
df_ord_itm=df_ord_itm.drop('is_campaign')
df_ord_itm=df_ord_itm.drop('product_campaign_id')


#Cleaning Data From Product....
df_prd=df_prd.filter(df_prd.product_id.isNotNull())
df_prd=df_prd.dropDuplicates(["product_id"])
df_prd=df_prd.drop("created_at")


#Cleaning Data From Seller....
df_seller=df_seller.filter(df_seller.seller_id.isNotNull())
df_seller=df_seller.dropDuplicates(["seller_id"])
df_seller=df_seller.drop("province","fbs_standard")
df_seller=df_seller.withColumn('join_date',df_seller['join_date'].cast('date'))






#Writing Data
logger.info("Writing Data To Silver....")
df_cus.write.mode("overwrite").parquet("/tmp/rutvik_proj/silver/shopee_cus")
df_ord_itm.write.mode("overwrite").parquet("/tmp/rutvik_proj/silver/shopee_ord_itm")
df_seller.write.mode("overwrite").parquet("/tmp/rutvik_proj/silver/shopee_seller")
df_prd.write.mode("overwrite").parquet("/tmp/rutvik_proj/silver/shopee_prd")
