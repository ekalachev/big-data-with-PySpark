from pyspark import SparkConf, SparkContext
import numpy as np


conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parseLine(line: str):
    fields = line.split(',')
    age = int(fields[2])
    num_fiends = int(fields[3])
    return (age, num_fiends)


lines = sc.textFile("../data/fakefriends.csv")
rdd = lines.map(parseLine)

# transformation operations
averagesByAge = rdd.mapValues(
    # makes: (age, (num_firneds, 1))
    lambda x: (x, 1)
).reduceByKey(
    # makes: (age, (num_fiends1 + num_fiends2, num1 + num2))
    lambda x, y: (x[0] + y[0], x[1] + y[1])
).mapValues(
    # makes: (age, np.round(num_fiends / num, 2))
    lambda x: np.round(x[0] / x[1], 2)
)

# action operation
results = averagesByAge.collect()

for result in results:
    print(result)
