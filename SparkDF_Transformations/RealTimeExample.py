from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

l = ['DEBUG,2015-2-6 16:24:07',
     'WARN,2016-7-26 18:54:43',
     'INFO,2012-10-18 14:35:1',
     'DEBUG,2012-4-26 14:26:5',
     'DEBUG,2013-9-28 20:27:1',
     'INFO,2017-8-20 13:17:27',
     'INFO,2015-4-13 09:28:17',
     'DEBUG,2015-7-17 00:49:2',
     'DEBUG,2014-7-26 02:33:0',
     'INFO,2016-1-13 09:51:57',
     'DEBUG,2015-1-14 08:55:3',
     'DEBUG,2016-1-20 03:47:0',
     'DEBUG,2013-7-8 21:00:50',
     'DEBUG,2012-5-22 11:43:5',
     'DEBUG,2013-3-20 06:14:5',
     'INFO,2015-8-8 20:49:22',
     'WARN,2015-1-14 20:05:00',
     'INFO,2017-6-14 00:08:35',
     'INFO,2016-1-18 11:50:14',
     'DEBUG,2017-7-1 12:55:02',
     'INFO,2014-2-26 12:34:21',
     'INFO,2015-7-12 11:13:47',
     'INFO,2017-4-15 01:20:18',
     'DEBUG,2016-11-2 20:19:2',
     'INFO,2012-8-20 10:09:44',
     'DEBUG,2014-4-22 21:30:4',
     'WARN,2013-12-6 17:54:15',
     'DEBUG,2017-1-12 10:47:0',
     'DEBUG,2016-6-25 11:06:4']

spark_config = (SparkConf()
                .setAppName('How to do it in Realtime')
                .setMaster('local[*]'))

data_schema = StructType([
    StructField('debug_level', StringType()),
    StructField('date_time', StringType())
])

spark = (SparkSession
         .builder
         .config(conf=spark_config)
         .getOrCreate())

spark.sparkContext.setLogLevel('ERROR')
# rdd = spark.sparkContext.parallelize(l).map(lambda x: x.split(','))
# df = rdd.toDF(schema=data_schema)
#
# df.createTempView('logs')
# df1 = spark.sql('SELECT debug_level,collect_list(date_time) FROM logs GROUP BY debug_level')
# df1 = spark.sql('SELECT debug_level,date_format(date_time,"MMMM") as month FROM logs')
# df1.createTempView('new_data_table')
# df2 = spark.sql('SELECT debug_level,month,COUNT(1) FROM new_data_table GROUP BY debug_level,month')
# df2.show()

df3 = spark.read.csv('/Users/rohith/RohithsWorkRelated/'
                     'DataEngineerRelated/TrendyTech/Week-12/biglog.txt', schema=data_schema, header=True)
df3.createTempView('logs')
mon_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
            'November',
            'December']
result1 = (spark.sql('''SELECT debug_level,
                              date_format(date_time,"MMMM") as month FROM logs''')
           .groupBy('debug_level')
           .pivot('month', mon_list)
           .count())

result1.show(100)
result1.printSchema()

spark.stop()
