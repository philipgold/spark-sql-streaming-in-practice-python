from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("StructuredNetworkWordCount") \
        .config("spark.ui.port", "44040" ) \
        .getOrCreate()

    df = spark.read.text("/Users/pg/Projects/myprojects/spark-sql-streaming-in-practice-python/data/songs/britney/britney.txt")
    df_stopwords = spark.read.text("/Users/pg/Projects/myprojects/spark-sql-streaming-in-practice-python/data/songs/britney/britney.txt")

    # Split the lines into words
    words = df.select(
        # explode turns each item in an array into a separate row
        explode(
            split(df.value, " ")
        ).alias('word')
    )

    cleanedWords = words.filter(~df.word.isin(*stop_words))

    # Generate running word count
    wordCounts = words.groupBy('word').count()
    # wordCounts.show()

    sortedwords = wordCounts.filter("`count` >= 10") \
    .sort('count', ascending=False)

    sortedwords.show()

    # Start running the query that prints the running counts to the console
    query = wordCounts \
        .write \
        .format('console')


    #
    # # Create DataFrame representing the stream of input lines from connection to host:port
    # lines = spark \
    #     .readStream \
    #     .format('socket') \
    #     .option('host', host) \
    #     .option('port', port) \
    #     .load()
    #
    # # Split the lines into words
    # words = lines.select(
    #     # explode turns each item in an array into a separate row
    #     explode(
    #         split(lines.value, ' ')
    #     ).alias('word')
    # )
    #
    # # Generate running word count
    # wordCounts = words.groupBy('word').count()
    #
    # # Start running the query that prints the running counts to the console
    # query = wordCounts \
    #     .writeStream \
    #     .outputMode('complete') \
    #     .format('console') \
    #     .start()
    #
    # query.awaitTermination()