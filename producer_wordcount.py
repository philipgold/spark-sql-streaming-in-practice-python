from kafka import KafkaProducer

if __name__ == "__main__":
    topic = 'wordcount'
    brokers = 'localhost:9092'



    # producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers=brokers)
    producer = KafkaProducer(bootstrap_servers=brokers)  # type: KafkaProducer
    # producer.send(KAFKA_TOPIC, {'foo': 'bar'})

    for _ in range(5):
        producer.send(topic, value="test")