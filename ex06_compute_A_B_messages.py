
'''1.	Create DF's of type A and B containing following columns:
    a.	Kafka TimeStamp (time arrival at consumer)
    b.	Event Time
    c.	Difference between event time and Kafka time
    d.	Event Value
'''
from pyspark.sql import SparkSession
from datetime import datetime, date, time

import os

from pyspark.sql.functions import from_json, current_timestamp, current_date, udf

from pyspark.sql.types import *

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 pyspark-shell'


if __name__ == "__main__":

    bootstrapServers = "localhost:9092"
    subscribeType = "subscribe"
    topics = "Cads"

    spark = SparkSession \
        .builder \
        .appName("StructuredKafkaWordCount") \
        .getOrCreate()


    # Disable log info
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Create Dataframe representing the stream of input json from kafka
    rawKafkaDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrapServers) \
        .option(subscribeType, topics) \
        .load() \
        .selectExpr("CAST(value AS STRING)", "CAST(timestamp as STRING)")



    rawKafkaDF.printSchema()

    # Convert to JSON and then parce json value

    cadsJsonSchema = StructType() \
        .add("EventName", "string") \
        .add("EventTime", "string") \
        .add("Value", "string")

    json_df = rawKafkaDF \
        .select(from_json("value", cadsJsonSchema)\
                .alias("parsed_json_values"), rawKafkaDF['timestamp'] \
                .alias("KafkaTimestamp"))

    json_df.printSchema()

    message_data = json_df.select("parsed_json_values.*", "KafkaTimestamp")

    message_data.printSchema()


    # Adding a column - Difference between event time and Kafka time
    my_func = udf(lambda event_time, kafka_timestamp:
                (datetime.strptime(kafka_timestamp, '%Y-%m-%d %H:%M:%S.%f') - datetime.strptime(event_time,
                                                                                               '%Y-%m-%d %H:%M:%S.%f')))

    new_col = my_func(message_data['EventTime'], message_data['KafkaTimestamp'])  # Column instance
    message_data1 = message_data.withColumn("new_col", new_col)

    # Start running the query that prints the running counts to the console
    query = message_data1 \
        .writeStream.outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    query.awaitTermination()