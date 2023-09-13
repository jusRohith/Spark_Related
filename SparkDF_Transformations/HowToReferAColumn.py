from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StringType, IntegerType, DateType, StructType, StructField
from pyspark.sql.functions import col, column, expr

spark_config = (SparkConf()
                .setAppName('how to refer a column')
                .setMaster('local[*]'))
input_data_schema = StructType([
    StructField('order_id', IntegerType()),
    StructField('order_date', DateType()),
    StructField('order_customer_id', IntegerType()),
    StructField('order_status', StringType())
])
spark = (SparkSession
         .builder
         .config(conf=spark_config)
         .getOrCreate())
orders_df = (spark
             .read
             .format('csv')
             .option('path', '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                             'TrendyTech/Week-11/Week-11_Datasets/orders.csv')
             .option('header', True)
             .schema(schema=input_data_schema)
             .load())

# orders_df.select('order_id', 'order_status').show()
# orders_df.select(column('order_date'), col('order_customer_id')).show()
# orders_df.select('order_id',column('order_date')).show()
# (orders_df
#  .select('order_id', expr('concat(order_status,"_STATUS")')
#          .alias('order_status'))
#  .show(truncate=False))
orders_df.selectExpr('order_id', 'concat(order_status,"_STATUS")').show()


spark.stop()