from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark_config = (SparkConf()
                .set('spark.app.name', 'countries')
                .set('spark.master', 'local[*]'))

table_schema = StructType([
    StructField('country', StringType()),
    StructField('week_num', IntegerType()),
    StructField('num_invoices', IntegerType()),
    StructField('total_quantity', IntegerType()),
    StructField('invoice_value', FloatType())
])
spark = (SparkSession.builder
         .config(conf=spark_config)
         .enableHiveSupport()
         .getOrCreate())
input_df = (spark.read
            .format('csv')
            .option('path', '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                            'TrendyTech/Week-11/Week11_Assignment_Dataset/windowdata.csv')
            .schema(table_schema)
            .option('header', True)
            .load())

spark.sql('CREATE DATABASE IF NOT EXISTS retail')

(input_df.write
 .format('csv')
 .mode('overwrite')
 .saveAsTable('retail.countries'))

for table in spark.catalog.listTables('retail'):
    print(table)
spark.stop()
