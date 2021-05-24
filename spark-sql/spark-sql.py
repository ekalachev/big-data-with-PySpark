from pyspark.sql import SparkSession, Row


# Create a Spark session
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(
        ID=int(fields[0]),
        name=str(fields[1].encode('utf-8')),
        age=int(fields[2]),
        num_friends=int(fields[3])
    )


lines = spark.sparkContext.textFile("../data/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
# Added cache because we're going to make a couple of requests.
schema_people = spark.createDataFrame(people).cache()
schema_people.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL query are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
    print(teen)

# We can use show to watch table.
teenagers.show(5)

# We can also use functions instead SQL queries.
schema_people.groupBy("age").count().orderBy("age").show(5)

spark.stop()
