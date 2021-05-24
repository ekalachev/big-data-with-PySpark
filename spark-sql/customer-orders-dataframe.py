from pyspark.sql import SparkSession, Row, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType


spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

schema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("amount_spent", FloatType(), True)
])

# Read the file as DataFrame
df = spark.read.schema(schema).csv("../data/customer-orders.csv")
df.printSchema()

# Select only cust_id and amount_spent
df = df.select("cust_id", "amount_spent")

# Calculate the total amount spent for each customer (sorted)
sorted_total_amount_spent = df.groupBy("cust_id").agg(
    func.round(func.sum(func.col("amount_spent")), 2)
        .alias("total_spent")
).sort("total_spent")

# Show the result
sorted_total_amount_spent.show(sorted_total_amount_spent.count())
