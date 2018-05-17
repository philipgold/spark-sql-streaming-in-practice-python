
"""
 Consumes messages from one or more topics in Kafka and does wordcount.
 Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
   comma-separated list of host:port.
   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
   'subscribePattern'.
   |- <assign> Specific TopicPartitions to consume. Json string
   |  {"topicA":[0,1],"topicB":[2,4]}.
   |- <subscribe> The topic list to subscribe. A comma-separated list of
   |  topics.
   |- <subscribePattern> The pattern used to subscribe to topic(s).
   |  Java regex string.
   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
   |  specified for Kafka source.
   <topics> Different value format depends on the value of 'subscribe-type'.

 Run the example
    `$ bin/spark-submit ex02_kafka_wordcount.py \
    host1:port1,host2:port2 subscribe topic1,topic2`
"""
from __future__ import print_function

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":

    bootstrapServers = "localhost:9092"
    subscribeType = "subscribe"
    topics = "wordcount"

    spark = SparkSession \
        .builder \
        .appName("StructuredKafkaWordCount") \
        .config("spark.ui.port", "44040" ) \
        .getOrCreate()


    # Disable log info
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    # Create DataSet representing the stream of input lines from kafka
    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrapServers) \
        .option(subscribeType, topics) \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    # Split the lines into words
    words = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ' ')
        ).alias('word')
    )

    # Generate running word count
    wordCounts = words.groupBy('word').count()

    # Start running the query that prints the running counts to the console
    query = wordCounts \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

    query.awaitTermination()