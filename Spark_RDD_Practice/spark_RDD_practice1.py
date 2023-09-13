from pyspark import SparkContext

sc = SparkContext('local[*]', 'rdd_practice')

rdd1 = sc.textFile(
    '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/TrendyTech/Week-9/Week9_Datasets/search_data.txt')

# Basic
# rdd2 = rdd1.map(lambda x: x.split())
# rdd3 = rdd2.flatMap(lambda x: x)
# rdd4 = rdd3.map(lambda x: (x, 1))
# rdd5 = rdd4.reduceByKey(lambda x, y: x + y)
# for i in rdd5.collect():
#     print(i)

rdd2 = (rdd1.flatMap(lambda x: x.split())
        .map(lambda x: (x, 1))
        .reduceByKey(lambda x, y: x + y)
        .sortByKey(ascending=True))
print(rdd2.getNumPartitions())

rdd2.saveAsTextFile('/Users/rohith/RohithsWorkRelated/HDFS_output/rdd_practice1')
