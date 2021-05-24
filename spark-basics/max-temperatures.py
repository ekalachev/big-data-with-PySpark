from pyspark import SparkContext, SparkConf


conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    statiob_id = fields[0]
    entry_type = fields[2]
    # convert to from Celsius to Fahrenheit
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (statiob_id, entry_type, temperature)


lines = sc.textFile("./data/1800.csv")

parsedLines = lines.map(parseLine)

# transformation operations
min_temps = parsedLines.filter(
    lambda x: "TMAX" in x[1]
).map(
    lambda x: (x[0], x[2])
).reduceByKey(
    # find the max value by key
    lambda x, y: max(x, y)
)

# action operations
results = min_temps.collect()

for result in results:
    print(f"{result[0]} \t {result[1]:.2f}F")
