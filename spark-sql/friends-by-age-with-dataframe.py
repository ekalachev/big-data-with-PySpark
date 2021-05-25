from pyspark.sql import SparkSession, Row, functions as func


spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

friends = (spark.read
           .option("header", "true")
           .option("inferSchema", "true")
           .csv("../data/fakefriends-header.csv"))

friends.printSchema()

friendsByAge = friends.select("age", "friends").groupBy("age")

# Sorted
friendsByAge.avg("friends").sort("age").show()

# Formated more nicely
friendsByAge.agg(func.round(func.avg("friends"), 2)).sort("age").show()

# With a custom column name
friendsByAge.agg(
    func.round(func.avg("friends"), 2).alias("friends_avg")
).sort("age").show()

spark.stop()
