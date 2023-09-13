from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master('local[*]')
         .appName('Parquet To DataFrame')
         .getOrCreate())

import_parquet_data = (spark.read
                       .option('path', '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                                       'TrendyTech/Week-11/Week-11_Datasets/users.parquet')
                       .load())

import_parquet_data.show()
import_parquet_data.printSchema()

