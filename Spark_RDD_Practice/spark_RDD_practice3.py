from pyspark import SparkContext

sc = SparkContext('local[*]', 'rdd_practice3')
input_data_rdd = sc.textFile(
    '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/TrendyTech/Week-9/Week9_Datasets/customerorders.csv')
# customer_id,product_id,price
customer_spent = (input_data_rdd
                  .map(lambda x: (x.split(',')[0], float(x.split(',')[2])))
                  .reduceByKey(lambda x, y: x + y)
                  .sortBy(lambda x: x[1], ascending=False)
                  .map(lambda x: (x[0], f'{x[1]:.2f}')))
for i in customer_spent.collect():
    print(i)
