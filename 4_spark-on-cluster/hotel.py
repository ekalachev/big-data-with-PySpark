from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType


def setup_spark():
    spark = SparkSession.builder.appName(
        "HotelWeather").master("local").getOrCreate()

    spark.conf.set(
        "fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")

    spark.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

    spark.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net",
                   "f3905ff9-16d4-43ac-9011-842b661d556d")

    spark.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",
                   "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT")

    spark.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net",
                   "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")

    return spark


spark = setup_spark()

hotel_schema = StructType([
    StructField("id", LongType(), False),
    StructField("address", StringType(), False),
    StructField("country", StringType(), False),
    StructField("city", StringType(), False),
    StructField("name", StringType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("geo_hash", StringType(), True)
])

weather_schema = StructType([
    StructField("lng", DoubleType(), False),
    StructField("lat", DoubleType(), False),
    StructField("geo_hash", StringType(), True),
    StructField("avg_tmpr_f", DoubleType(), False),
    StructField("avg_tmpr_c", DoubleType(), False),
    StructField("wthr_date", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False)
])

hotel_df = spark.read.option("header", "true") \
    .parquet("abfss://m05sparkbasics@bd201stacc.dfs.core.windows.net/hotels")

hotel_df.show()

# spark-submit --packages org.apache.hadoop:hadoop-common:3.3.0,org.apache.hadoop:hadoop-azure:3.3.0,com.microsoft.azure:azure-storage:8.6.6 .\hotel.py
