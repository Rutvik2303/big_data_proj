
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from logger_config import get_logger
logger = get_logger(__name__)

logger.info("Spark Session Start....")
spark = SparkSession.builder \
    .appName("gold") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


logger.info("Readind Data....")
# Read data from HDFS (assuming CSV format)
df_cus = spark.read.parquet('/tmp/rutvik_proj/silver/shopee_cus')
df_ord_itm = spark.read.parquet('/tmp/rutvik_proj/silver/shopee_ord_itm')
df_prd=spark.read.parquet('/tmp/rutvik_proj/silver/shopee_prd')
df_seller=spark.read.parquet('/tmp/rutvik_proj/silver/shopee_seller')





logger.info("Creating Temp_view....")
#Creating Temp_view
df_cus.createOrReplaceTempView('cust')
df_prd.createOrReplaceTempView('prd')
df_ord_itm.createOrReplaceTempView('ord_itm')
df_seller.createOrReplaceTempView('seller')



#=======================================================================Insight:===========================

logger.info("Creating df for each Insight....")
#sale per day
df_higest_sales_day=spark.sql("select ord_itm.order_date,round(sum(ord_itm.line_total+ord_itm.shipping_fee_item),2) as total_sales from ord_itm group by ord_itm.order_date")


#commition per product
df_total_com_prd=spark.sql("select product_name, round(sum(total_commission),2) as total_commission from (select ord_itm.product_id, first(product_name) as product_name, sum(ord_itm.commission_amount) as total_commission from ord_itm join prd on ord_itm.product_id=prd.product_id group by ord_itm.product_id) group by product_name")

#order per city
df_total_ord_city=spark.sql("select city, count(*) as total_orders from(select ord_itm.customer_id,first(city) as city, count(DISTINCT ord_itm.order_id) as total_orders from ord_itm join cust on ord_itm.customer_id=cust.customer_id where ord_itm.item_status='Completed' group by ord_itm.customer_id) group by city")

# most return product
df_cus_return=spark.sql("select * from(select prd.product_id,first(product_name) as product_name,round(SUM(CASE WHEN ord_itm.item_status = 'Refunded' THEN ord_itm.quantity ELSE 0 END)  / SUM(ord_itm.quantity) *100,2)AS return_rate,round(sum(ord_itm.commission_amount),2) as commission_lost from prd join ord_itm on ord_itm.product_id=prd.product_id group by prd.product_id)where return_rate>50")

#below avd saller
df_worst_seller=spark.sql("select shope_name,total_sales from(select first(shop_name) as shope_name,round(sum(ord_itm.line_total),2) as total_sales,avg(sum(ord_itm.line_total)) over() as avg_sales from ord_itm join prd on ord_itm.product_id=prd.product_id join seller on prd.seller_id=seller.seller_id group by prd.seller_id )where total_sales<avg_sales")




logger.info("Writing to hivr...")

df_higest_sales_day.write.mode("overwrite").saveAsTable("rutvikdb.sale_per_day")
df_total_com_prd.write.mode("overwrite").saveAsTable("rutvikdb.com_prd")
df_total_ord_city.write.mode("overwrite").saveAsTable("rutvikdb.ord_city")
df_cus_return.write.mode("overwrite").saveAsTable("rutvikdb.prd_return")
df_worst_seller.write.mode("overwrite").saveAsTable("rutvikdb.worst_seller")









logger.info("Writing to DB")
# Write data to PostgreSQL
df_higest_sales_day.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://13.42.152.118:5432/testdb") \
  .option("dbtable", "rutvik.sale_per_day") \
  .option("user", "admin") \
  .option("password", "admin123") \
  .option("driver", "org.postgresql.Driver") \
  .mode("overwrite") \
  .save()
  
  
df_total_com_prd.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://13.42.152.118:5432/testdb") \
  .option("dbtable", "rutvik.com_prd") \
  .option("user", "admin") \
  .option("password", "admin123") \
  .option("driver", "org.postgresql.Driver") \
  .mode("overwrite") \
  .save()
  
  
  
  
df_total_ord_city.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://13.42.152.118:5432/testdb") \
  .option("dbtable", "rutvik.ord_city") \
  .option("user", "admin") \
  .option("password", "admin123") \
  .option("driver", "org.postgresql.Driver") \
  .mode("overwrite") \
  .save()
  
  
  
  
df_cus_return.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://13.42.152.118:5432/testdb") \
  .option("dbtable", "rutvik.prd_return") \
  .option("user", "admin") \
  .option("password", "admin123") \
  .option("driver", "org.postgresql.Driver") \
  .mode("overwrite") \
  .save()
  
  
  
  
df_worst_seller.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://13.42.152.118:5432/testdb") \
  .option("dbtable", "rutvik.worst_seller") \
  .option("user", "admin") \
  .option("password", "admin123") \
  .option("driver", "org.postgresql.Driver") \
  .mode("overwrite") \
  .save()

  
  
  
  
  
  
  
  
  
  
  

spark.stop()