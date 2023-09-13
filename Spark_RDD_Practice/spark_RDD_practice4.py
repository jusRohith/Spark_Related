from pyspark import SparkContext

# user_id movie_id rating time_stamp
sc = SparkContext('local[*]', 'rdd_practice4')
input_data_rdd = sc.textFile(
    '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/TrendyTech/Week-9/Week9_Datasets/moviedata.data')

# way1
# final_output = (input_data_rdd
#                 .map(lambda x: x.split())
#                 .map(lambda x: (x[2], 1))
#                 .reduceByKey(lambda x, y: x + y)
#                 .sortByKey())

# way2
final_output = (input_data_rdd
                .map(lambda x: x.split())
                .map(lambda x: x[2])
                .countByValue())  # this function is an action not transformation it returns Dict not RDD.
for i, j in final_output.items():
    print(i, j)
