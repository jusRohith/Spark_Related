from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import StructType

spark = (SparkSession
         .builder
         .master('local[*]')
         .appName('Demo')
         .getOrCreate())
input_data = (spark
              .read
              .text('/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                    'TrendyTech/Week-9/Week9_Datasets/search_data.txt'))
# rdd = (spark
#        .sparkContext
#        .textFile('/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
#                  'TrendyTech/Week-9/Week9_Datasets/search_data.txt').map(lambda x: x.split(' ')))
#
# rdd1 = (rdd
#         .flatMap(lambda x: x)
#         .map(lambda x: (x, 1))
#         .reduceByKey(lambda x, y: x + y)
#         .sortBy(lambda x: x[1],False))
#
# print(rdd1.collect())


