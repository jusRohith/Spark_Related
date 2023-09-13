from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import IntegerType, StringType


# User Defined Functions
# If we are working with RDD's(Lower Level Constructs)  then simply we can use this function in transformations
# easily as an anonymous functions, but here we are dealing with DataFrames (Higher Level Constructs)
# so, we can't use UDF's directly, first we have to register this UDF then only we can use with DF's
# We can register UDF's using @udf and udf() in pyspark.sql.functions


# user_defined_function 1
@udf(returnType=StringType())
def age_check(age):
    return 'Y' if age > 18 else 'N'


# this above function is written in Column object expression, this function will not be registered in Catalog

# user_defined_function 2
word_len = udf(lambda s: len(s), IntegerType())


# this above function is written in Column object expression, this function will not be registered in Catalog

# user_defined_function 3
def city_len(city_name):
    return len(city_name)


spark_config = (SparkConf()
                .setAppName('UDF creation')
                .setMaster('local[*]'))

spark = (SparkSession
         .builder
         .config(conf=spark_config)
         .getOrCreate())
input_df = (spark
            .read
            .csv('/Users/rohith/RohithsWorkRelated/DataEngineerRelated/TrendyTech/Week-12/dataset1',
                 inferSchema=True, ))
df1 = input_df.toDF('name', 'age', 'city')

df2 = df1.select('name',
                 word_len('name').alias('name_length'),
                 'age',
                 age_check('age').alias('above_18'),
                 'city')

# Whenever you want to add new column to existing df then you should use withColumn() function

df2.show()
df3 = df1.withColumn('name_length', word_len('name'))
df4 = df1.withColumn('above_18', age_check('age'))

spark.udf.register('citynamelength', city_len, IntegerType())
# Here we are registering the UDF in Catalog
for x in spark.catalog.listFunctions():
    print(x)

df3 = df2.withColumn('city_name_length', expr('citynamelength(city)'))  # UDF SQL expression
df3.show()

spark.stop()
