from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName('JSON_to_Dataframe')
         .master('local[*]')
         .getOrCreate())
inputData_df = (spark.read
                .format('JSON')
                .option('path', '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                                'TrendyTech/Week-11/Week-11_Datasets/players.json')
                .load())
inputData_df.show()
inputData_df.printSchema()
