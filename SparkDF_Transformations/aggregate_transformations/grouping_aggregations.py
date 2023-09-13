# Grouping Aggregation Requirement
# --------------------------------
# Group the data based on  invoice number and country
#
# I want total quantity for each group, sum of invoice value --> sum of (quantity * unit_price)
#
# 1. do it using column expression
# 2. do it using string expression
# 3. do it using spark sql


from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import unix_timestamp, sum, expr

spark_config = (SparkConf()
                .set('spark.app.name', 'grouping aggregations')
                .set('spark.master', 'local[*]'))

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

spark = SparkSession.builder.config(conf=spark_config).getOrCreate()

orders_data_df = (spark.read
                  .format('csv')
                  .option('path', '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/'
                                  'TrendyTech/Week-12/order_data.csv')
                  .schema(schema=data_schema)
                  .option('header', True)
                  .load())
data_modified_df = (orders_data_df
                    .withColumn('InvoiceDate', unix_timestamp('InvoiceDate', "dd-MM-yyyy H.mm")
                                .cast(TimestampType())))

# Column Object Expression
summary_df = (data_modified_df.groupBy(['InvoiceNo', 'Country'])
              .agg(sum('Quantity').alias('Total Quantity'),
                   sum(expr('Quantity * UnitPrice')).alias('Invoice Value')))

# String Expression

# summary_df1 = (data_modified_df
#                .groupBy('Country', 'InvoiceNo')
#                .agg(expr('sum(Quantity))').alias('Total Quantity'),
#                     expr('sum(Quantity * UnitPrice)').alias('Invoice Value'))
# # summary_df1.show()

# Spark SQL
data_modified_df.createOrReplaceTempView('sales')

summary_df2 = spark.sql("""
                        SELECT Country,
                                InvoiceNo,
                                SUM(Quantity) AS `Total Quantity`,
                                SUM(Quantity * UnitPrice) AS `Invoice Value`
                                FROM sales
                        GROUP BY Country,InvoiceNo """)

spark.stop()
