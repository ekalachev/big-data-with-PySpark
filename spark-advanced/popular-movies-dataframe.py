from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create schema when reading u.data
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Load up movie data as dataframe
movies_df = spark.read.option("sep", "\t").schema(
    schema).csv("../data/ml-100k/u.data")

# Sort all movies by popularity
top_movies_df = movies_df.groupBy(
    "movie_id").count().orderBy(func.desc("count"))

# Grab the top 10
top_movies_df.show(10)

spark.stop()
