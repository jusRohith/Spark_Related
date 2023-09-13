from pyspark import SparkContext

sc = SparkContext('local[*]', 'rdd_practice5')
sc.setLogLevel('INFO')
input_data_rdd = sc.textFile(
    '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/TrendyTech/Week-9/Week9_Datasets/friendsdata.csv')
# rowid,person_name,person_age,linkedin_connections_count

rdd1 = (input_data_rdd
        .map(lambda x: x.split(','))
        .map(lambda x: (x[2], (int(x[3]), 1))))
rdd2 = rdd1.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
rdd3 = (rdd2.map(lambda x: (x[0], float(f'{(x[1][0] / x[1][1]):.2f}')))
        .sortBy(lambda x: x[1], ascending=False))
print(rdd3.collect())
