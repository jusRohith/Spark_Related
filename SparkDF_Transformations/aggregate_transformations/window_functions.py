from pyspark.sql import SparkSession, Window
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
from pyspark.sql.functions import unix_timestamp, count, sum

spark_config = (SparkConf()
                .setAppName('window functions')
                .setMaster('local[*]'))
data_schema = StructType([
    StructField('country', StringType()),
    StructField('week_num', IntegerType()),
    StructField('no.of.invoices', IntegerType()),
    StructField('total_quantity', IntegerType()),
    StructField('invoice_value', FloatType())
])
spark = (SparkSession
         .builder
         .config(conf=spark_config)
         .getOrCreate())

input_data_DF = (spark.read
                 .format('csv')
                 .option('path', '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                                 'TrendyTech/Week-11/Week11_Assignment_Dataset/windowdata.csv')
                 .schema(schema=data_schema)
                 .load())

window = (Window.partitionBy('country')
          .orderBy('week_num')
          .rowsBetween(Window.unboundedPreceding, Window.currentRow))

input_data_DF.withColumn('running_column', sum('invoice_value').over(window)).show()

spark.stop()
