from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType


spark = SparkSession.builder.appName(
    "HotelWeather").master("local[*]").getOrCreate()

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


def extract_hotel_weather():
    hotel_weather_schema = StructType([
        StructField("id", StringType(), False),
        StructField("address", StringType(), False),
        StructField("country", StringType(), False),
        StructField("city", StringType(), False),
        StructField("name", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("geoHash", StringType(), False),
        StructField("avg_tmpr_c", DoubleType(), False),
        StructField("avg_tmpr_f", DoubleType(), False),
        StructField("wthr_date", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False)
    ])

    hotel_weather_df = spark.read \
        .format("parquet") \
        .option("header", "true") \
        .schema(hotel_weather_schema) \
        .load("abfss://m06sparksql@bd201stacc.dfs.core.windows.net/hotel-weather")


def extract_expedia():
    expedia_schema = StructType([
        StructField("id", LongType(), True),
        StructField("date_time", StringType(), True),
        StructField("site_name", IntegerType(), True),
        StructField("posa_continent", IntegerType(), True),
        StructField("user_location_country", IntegerType(), True),
        StructField("user_location_region", IntegerType(), True),
        StructField("user_location_city", IntegerType(), True),
        StructField("orig_destination_distance", DoubleType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("is_mobile", IntegerType(), True),
        StructField("is_package", IntegerType(), True),
        StructField("channel", IntegerType(), True),
        StructField("srch_ci", StringType(), True),
        StructField("srch_co", StringType(), True),
        StructField("srch_adults_cnt", IntegerType(), True),
        StructField("srch_children_cnt", IntegerType(), True),
        StructField("srch_rm_cnt", IntegerType(), True),
        StructField("srch_destination_id", IntegerType(), True),
        StructField("srch_destination_type_id", IntegerType(), True),
        StructField("hotel_id", LongType(), True)
    ])

    expedia_df = spark.read \
        .format("avro") \
        .option("header", "true") \
        .schema(expedia_schema) \
        .load("abfss://m06sparksql@bd201stacc.dfs.core.windows.net/expedia")

    return expedia_df


# spark-submit --packages org.apache.hadoop:hadoop-client:3.2.1,org.apache.hadoop:hadoop-azure:3.2.1,org.apache.hadoop:hadoop-azure-datalake:3.2.1,com.azure:azure-identity:1.2.3,com.azure:azure-storage-file-datalake:12.4.1,com.azure:azure-storage-blob:12.10.2,com.microsoft.azure:azure-data-lake-store-sdk:2.3.9,com.microsoft.azure:azure-keyvault-core:1.2.4,com.microsoft.azure:azure-storage:8.6.6,com.thoughtworks.paranamer:paranamer:2.3,org.apache.spark:spark-avro_2.12:3.1.2 hotel.py
