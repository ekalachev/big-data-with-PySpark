from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

# Create schema when reading
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

names = spark.read.schema(schema).option(
    "sep", " ").csv("../data/Marvel+Names")

lines = spark.read.text("../data/Marvel+Graph")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

min_connections = connections.agg(func.min("connections")).first()[0]

most_obscure_superheroes = connections \
    .filter(func.col("connections") == min_connections) \
    .join(names, "id") \
    .select("name", "connections") \
    .sort("connections")

print("The following characters have only", min_connections, "connection(s):")

most_obscure_superheroes.show()
