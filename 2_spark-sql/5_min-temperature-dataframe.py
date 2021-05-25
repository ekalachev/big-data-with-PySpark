from pyspark.sql import SparkSession, Row, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def celcium_to_fahrenheit(temperature):
    return temperature * 0.1 * (9.0 / 5.0) + 32.0


spark = SparkSession.builder.appName("MinTemperature").getOrCreate()

schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("temperature", FloatType(), True)
])

# Read the file as DataFrame
df = spark.read.schema(schema).csv("../data/1800.csv")
df.printSchema()

# Filter out all but TMIN entries
min_temps = df.filter(df.measure_type == "TMIN")

# Select only station_id and temperature
station_temps = min_temps.select("station_id", "temperature")

# Aggreagte to find minimum temperature for every station
min_temps_by_station = station_temps.groupBy("station_id").min("temperature")
min_temps_by_station.show()

# Convert temperature to Fahrenheit and sort the dataset
min_temps_by_station_f = min_temps_by_station.withColumn(
    "temperature",
    func.round(celcium_to_fahrenheit(func.col("min(temperature)")), 2)
).select("station_id", "temperature").sort("temperature")

# Collect, format, and print the results
results = min_temps_by_station_f.collect()

for result in results:
    print(f"{result[0]}\t{result[1]:.2f} F")

spark.stop()
