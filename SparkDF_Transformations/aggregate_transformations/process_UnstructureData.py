from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import re

df_schema = StructType([
    StructField('order_id', IntegerType()),
    StructField('date', DateType()),
    StructField('customer_id', IntegerType()),
    StructField('order_status', StringType())

])

spark_config = (SparkConf()
                .setAppName('processing unstructured data')
                .setMaster('local[*]'))
spark = (SparkSession
         .builder
         .config(conf=spark_config)
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
unstructured_rdd = (spark
                    .sparkContext
                    .textFile('/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                              'TrendyTech/Week-11/Week-11_Datasets/orders_new.csv'))

rdd = (unstructured_rdd
       .map(lambda x: re.split('^(\S+) (\S+)\t(\S+),(\S+)', x))
       .map(lambda x: list((filter(lambda a: a != '', x))))  # removing empty strings from each list
       .map(lambda x: [int(x[0]), datetime.strptime(x[1], '%Y-%m-%d').date(), int(x[2]), x[3]]))
data_DF = rdd.toDF(schema=df_schema)
# data_DF.select('order_id').show()
data_DF.groupby('order_status').count().show()


spark.stop()
