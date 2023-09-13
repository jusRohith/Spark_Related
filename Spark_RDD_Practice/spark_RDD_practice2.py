from pyspark import SparkContext

sc = SparkContext('local[*]', 'word_count')
input_rdd = sc.textFile(
    '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/TrendyTech/Week-9/Week9_Datasets/search_data.txt')

rdd1 = (input_rdd
        .flatMap(lambda x: x.split())
        .map(lambda x: (x.upper(), 1)))
words_count = (rdd1
               .reduceByKey(lambda x, y: x + y)
               .map(lambda x: (x[1], x[0]))
               # if use this sort by then there will be shuffling of data which is too costly
               # .sortBy(lambda x: x[1], ascending=False))
               .sortByKey(ascending=False)
               .map(lambda x: (x[1], x[0])))

for i in words_count.collect():
    print(i[0])
