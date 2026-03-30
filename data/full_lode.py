from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rutu_full_lode").getOrCreate()

jdbc_url = "jdbc:postgresql://13.42.152.118:5432/testdb"

properties = {
"user": "admin",
"password": "admin123",
"driver": "org.postgresql.Driver"
}

#creating data frame

df_cus = spark.read.jdbc(url=jdbc_url,table="rutvik.shopee_customers_th",properties=properties)
df_ord_itm = spark.read.jdbc(url=jdbc_url,table="rutvik.shopee_order_items_th",properties=properties)

df_ord=spark.read.csv('/home/ec2-user/250226batch/rutvik/project/data/shopee_orders_th.csv',header=True,inferSchema=True)
df_cmp=spark.read.csv('/home/ec2-user/250226batch/rutvik/project/data/shopee_campaigns_thailand.csv',header=True,inferSchema=True)
df_prd=spark.read.csv('/home/ec2-user/250226batch/rutvik/project/data/shopee_products_thailand.csv',header=True,inferSchema=True)
df_seller=spark.read.csv('/home/ec2-user/250226batch/rutvik/project/data/shopee_sellers_thailand.csv',header=True,inferSchema=True)
df_prd_cmp=spark.read.csv('/home/ec2-user/250226batch/rutvik/project/data/shopee_product_campaign_thailand.csv',header=True,inferSchema=True)



#df.write.mode("overwrite").saveAsTable("/tmp/r_output/r_customer")
df_cus.write.mode("overwrite").parquet("/tmp/rutvik_proj/bronze/shopee_cus")
df_ord_itm.write.mode("overwrite").parquet("/tmp/rutvik_proj/bronze/shopee_ord_itm")
df_ord.write.mode("overwrite").parquet("/tmp/rutvik_proj/bronze/shopee_ord")
df_cmp.write.mode("overwrite").parquet("/tmp/rutvik_proj/bronze/shopee_cmp")
df_seller.write.mode("overwrite").parquet("/tmp/rutvik_proj/bronze/shopee_seller")
df_prd.write.mode("overwrite").parquet("/tmp/rutvik_proj/bronze/shopee_prd")
df_prd_cmp.write.mode("overwrite").parquet("/tmp/rutvik_proj/bronze/shopee_prd_cmp")