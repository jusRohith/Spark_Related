# Simple Aggregations
# ====================
# 1. Load the file and create DataFrame. I should do it using standard dataframe reader API.
# simple_agreegrations : total_number_rows,total_quantity,avg_unit_price,numberOfUniqueInvoices
# 2. calculate using column object expression
# 3. do the same using string expression
# 4. do it using spark sql.

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import unix_timestamp, col, asc, count_distinct, countDistinct, count, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType

spark_config = (SparkConf()
                .set('spark.app.name', 'simple aggregations')
                .set('master', 'local[*]'))
data_schema = StructType([
    StructField('InvoiceNo', IntegerType()),
    StructField('StockCode', IntegerType()),
    StructField('Description', StringType()),
    StructField('Quantity', IntegerType()),
    StructField('InvoiceDate', StringType()),
    StructField('UnitPrice', FloatType()),
    StructField('CustomerID', IntegerType()),
    StructField('Country', StringType())
])

spark = (SparkSession
         .builder
         .config(conf=spark_config)
         .getOrCreate())

spark.sparkContext.setLogLevel('ERROR')

df = (spark
      .read
      .format('csv')
      .option('path', '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                      'TrendyTech/Week-12/order_data.csv')
      .option('header', True)
      .option("dateFormat", 'dd-MM-yyyy H.mm')
      .schema(schema=data_schema)
      .load())
# df.show()
# df.printSchema()
df_with_time_stamp = df.withColumn(
    "InvoiceDate",
    unix_timestamp('InvoiceDate', "dd-MM-yyyy H.mm").cast(TimestampType()))
# df_with_time_stamp.show()
# df_with_time_stamp.printSchema()

# Object Expression
df_with_time_stamp.select(
    count('*').alias('no.of.records'),
    countDistinct('InvoiceNo').alias('count_distinct'),
    avg('UnitPrice').alias('Avg_Price'),
    sum('Quantity').alias('total_quantity')
).show()

# String Expression
df_with_time_stamp.selectExpr(
    'count(*) as row_count',
    'count(Distinct(InvoiceNo)) as count_distinct',
    'avg(UnitPrice) as avg_price',
    'sum(Quantity) as total_quantity'
).show()

# Using Spark SQL
df_with_time_stamp.createOrReplaceTempView('sales')
spark.sql('SELECT COUNT(*),'
          'COUNT(DISTINCT(InvoiceNo)),'
          'AVG(UnitPrice),'
          'SUM(Quantity)'
          'from sales').show()

spark.stop()
