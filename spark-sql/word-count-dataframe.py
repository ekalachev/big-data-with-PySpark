from pyspark.sql import SparkSession, Row, functions as func


spark = SparkSession.builder.appName("WordCount").getOrCreate()

input_df = spark.read.text("../data/book.txt")

# Split using a regular expression that extracts words
words = input_df.select(
    func.explode(func.split(input_df.value, "\\W+")).alias("word"))
words = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Count up the occurrences of each wprd
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results
wordCountsSorted.show(wordCountsSorted.count())

spark.stop()
