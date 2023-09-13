from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .master('local[*]')
         .appName('Creating First Spark Session and DataFrame')
         .getOrCreate())
# There are two ways you can create Dataframes
# 1. Using format() which preferred in industry
# 2. And the other one is directly using spark.read.csv (or) spark.read.parquet etc.,
orders_df = spark.read.csv('/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                           'TrendyTech/Week-11/Week-11_Datasets/orders.csv', header=True)
# orders_df.show()  # by default it will show you 20 records only, if you want you can change it

new_orders_df = (spark.read
                 .format('csv')
                 .option('path', '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                                 'TrendyTech/Week-11/Week-11_Datasets/orders_new.csv')
                 .option('inferSchema', True)  # inferSchema not preferable in production
                 .option('header', True)
                 .load())
# new_orders_df.show()
# new_orders_df.printSchema()


grouped_data_DF = (orders_df
                   .where('order_customer_id > 10000')
                   .select('order_id', 'order_customer_id')
                   .groupBy('order_customer_id')
                   .count())

grouped_data_DF.show()
# input_data_DF.printSchema()
spark.stop()
