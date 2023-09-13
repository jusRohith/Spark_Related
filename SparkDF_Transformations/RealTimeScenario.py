# Requirement
# ---------------
from datetime import datetime

# 1.We have to create a list
# 2.From list I want to create a dataframe (order_id,order_date,customer_id,status)
# 3.Convert order_date field to epoch timestamp(unix timestamp) - no.of seconds after 1st jan 1970
# 4.Create a new column with the name 'new_id' and make sure it has unique id's
# 5.drop duplicates (order_date,customer_id)
# 6.Then we have to drop the order_id column
# 7.sort it by using new order_id

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import unix_timestamp, col, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

data_schema = StructType([
    StructField('order_id', IntegerType()),
    StructField('order_date', DateType()),
    StructField('customer_id', IntegerType()),
    StructField('status', StringType())
])
spark_config = (SparkConf()
                .setMaster('local[*]')
                .setAppName('real time scenario'))
spark = (SparkSession
         .builder
         .config(conf=spark_config)
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
column_header = ['order_id', 'order_date', 'customer_id', 'order_status']

l = [(1, '2013-07-25', 11599, 'CLOSED'),
     (2, '2014-07-25', 256, 'PENDING_PAYMENT'),
     (3, '2016-07-25', 12111, 'COMPLETE'),
     (4, '2013-07-25', 11599, 'CLOSED'),
     (5, '2019-07-25', 11318, 'COMPLETE'),
     (6, '2009-07-25', 7130, 'COMPLETE'),
     (7, '2018-07-25', 4530, 'COMPLETE'),
     (8, '2021-07-25', 2911, 'PROCESSING'),
     (9, '2013-07-25', 5657, 'PENDING_PAYMENT')]
# data_df = (spark.sparkContext.parallelize(l)
#            .map(lambda x: [int(x.split(',')[0]), datetime.strptime(x.split(',')[1], '%Y-%m-%d'),
#                            int(x.split(',')[2]), x.split(',')[3]])
#            .toDF(schema=data_schema))
data_df = spark.createDataFrame(l, schema=column_header)
data_df1 = (data_df
            .withColumn('order_date', unix_timestamp(col('order_date')
                                                     .cast(DateType())))
            .withColumn('new_id', monotonically_increasing_id()))

drop_duplicate_df = data_df1.dropDuplicates(['order_date', 'customer_id'])
drop_orderid_col = drop_duplicate_df.drop('order_id').sort('new_id')
drop_orderid_col.show()
drop_orderid_col.printSchema()

spark.stop()
