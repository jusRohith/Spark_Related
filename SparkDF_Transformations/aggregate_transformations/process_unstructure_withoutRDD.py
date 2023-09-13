from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark_config = (SparkConf()
                .setAppName('process unstructure data without using RDD')
                .setMaster('local[*]'))

spark = (SparkSession
         .builder
         .config(conf=spark_config)
         .getOrCreate())

my_regex = r'^(\S+) (\S+)\t(\S+),(\S+)'
lines_df = spark.read.text('/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                           'TrendyTech/Week-11/Week-11_Datasets/orders_new.csv')

final_df = lines_df.select(regexp_extract('value', my_regex, 1).alias('order_id'),
                           regexp_extract('value', my_regex, 2).alias('date'),
                           regexp_extract('value', my_regex, 3).alias('customer_id'),
                           regexp_extract('value', my_regex, 4).alias('status'))

final_df.groupby('status').count().show()

spark.stop()
