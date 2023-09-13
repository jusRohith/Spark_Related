from pyspark import SparkConf
from pyspark.sql import SparkSession

# There are two ways you can create the Spark Session
# 1.Without SparkConf
# 2.With SparkConf (You should import SparkConf from pyspark package)

# Creating Spark Session Without using SparkConf
# spark = (SparkSession.builder
#          .master('local[*]')
#          .appName('dataframe creation example')
#          .getOrCreate())

# Creating Spark Session using SparkConf
my_conf = (SparkConf()
           .set('spark.app.name', 'dataframe creation example')
           .set('spark.master', 'local[*]'))

spark = (SparkSession.builder
         .config(conf=my_conf)
         .getOrCreate())

print(spark, type(spark))

spark.stop()
