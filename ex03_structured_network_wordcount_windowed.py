"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network.
 Usage: structured_network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Structured Streaming
   would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_network_wordcount.py
    localhost 9999`
"""

from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

if __name__ == "__main__":
    host = "localhost"
    port = 9999
    windowSize = 10
    slideSize = 5
    if slideSize > windowSize:
        print("<slide duration> must be less than or equal to <window duration>", file=sys.stderr)
    windowDuration = '{} seconds'.format(windowSize)
    slideDuration = '{} seconds'.format(slideSize)

    spark = SparkSession \
        .builder \
        .appName("StructuredNetworkWordCountWindowed") \
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark \
        .readStream \
        .format('socket') \
        .option('host', host) \
        .option('port', port) \
        .option('includeTimestamp', 'true') \
        .load()

    # Split the lines into words, retaining timestamps
    # split() splits each line into an array, and explode() turns the array into multiple rows
    words = lines.select(
        explode(split(lines.value, ' ')).alias('word'),
        lines.timestamp
    )

    # Group the data by window and word and compute the count of each group
    windowedCounts = words.groupBy(

        window(words.timestamp, windowDuration, slideDuration),
        words.word
    ).count().orderBy('window')

    # Start running the query that prints the windowed word counts to the console
    query = windowedCounts \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .option('truncate', 'false') \
        .start()

    query.awaitTermination()
