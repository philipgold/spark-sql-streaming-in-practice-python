
r"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999` on Linux
    'nc -l -p 9999' on Windows

 and then run the example
    `$ bin/spark-submit ex00_dstream_network_wordcount.py localhost 9999`
"""
from __future__ import print_function

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    host = "localhost"
    port = 9999

    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    lines = ssc.socketTextStream(hostname=host, port=port)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()