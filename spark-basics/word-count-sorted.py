from pyspark import SparkContext, SparkConf
import regex as re


def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

text = sc.textFile("./data/book.txt")

words_count = (text
               .flatMap(normalize_words)
               .map(lambda x: (x, 1))
               .reduceByKey(lambda x, y: x + y)
               .map(lambda x: (x[1], x[0]))
               .sortByKey()
               .collect())

for count, word in words_count:
    clean_word = word.encode('ascii', 'ignore')
    if clean_word:
        print(f"{clean_word}:\t\t{count}")
