from pyspark import SparkContext, SparkConf


def parseLine(line):
    fields = line.split(',')
    return int(fields[0]), float(fields[2])


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

orders = sc.textFile("./data/customer-orders.csv")

sorted_orders = (orders
                 .map(parseLine)
                 .reduceByKey(lambda x, y: x + y)
                 .map(lambda x: (x[1], x[0]))  # reverse for sorting
                 .sortByKey()
                 .map(lambda x: (x[1], x[0]))  # reverse back
                 .collect())

for user, order_sum in sorted_orders:
    print(f"{user}:\t$ {round(order_sum, 2)}")
