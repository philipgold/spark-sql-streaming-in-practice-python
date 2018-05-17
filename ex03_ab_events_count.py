"""
 1.	Create DF's of type A and B containing following columns:
    a.	Kafka TimeStamp (time arrival at consumer)
    b.	Event Time
    c.	Difference between event time and Kafka time
    d.	Event Value
 Results DF: A1 and B1


 Run the example
    `$ bin/spark-submit ex03_ab_events_count.py \
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
    topics = "Cads"

    spark = SparkSession \
        .builder \
        .appName("StructureABEventsCount") \
        .config("spark.ui.port", "44041" ) \
        .getOrCreate()

    # Disable log info
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
