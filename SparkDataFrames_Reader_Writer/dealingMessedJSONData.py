from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master('local[*]')
         .appName('Dealing Messed JSON Data')
         .getOrCreate())

# By default it will be in PERMISSIVE mode

JSON_data_df = (spark.read
                .format('JSON')
                .option('inferSchema', True)
                .option('path', '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                                'TrendyTech/Week-11/Week-11_Datasets/players_messed.json')
                .option('mode','PERMISSIVE')  #Default mode
                .load())

JSON_data_df.show(truncate=False)
JSON_data_df.printSchema()



# DROPMALFORMED mode

# json_data_df = (spark.read
#                 .format('JSON')
#                 .option('path', '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
#                                 'TrendyTech/Week-11/Week-11_Datasets/players_messed.json')
#                 .option('inferSchema', True)
#                 .option('mode', 'DROPMALFORMED')
#                 .load())
# json_data_df.show(20, False)
# json_data_df.printSchema()

# FAILFAST mode

# json_data_df = (spark.read
#                 .format('JSON')
#                 .option('path', '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
#                                 'TrendyTech/Week-11/Week-11_Datasets/players_messed.json')
#                 .option('inferSchema',True)
#                 .option('mode','FAILFAST')
#                 .load())
# json_data_df.show()
# json_data_df.printSchema()
