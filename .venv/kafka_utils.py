from confluent_kafka import Consumer, Producer
from config import remote_kafka_config, local_kafka_config, SOURCE_TOPIC, TARGET_TOPIC

# Consumer for remote Kafka
source_consumer_conf = {
    **remote_kafka_config,
    'group.id': 'source-consumer-group',
    'auto.offset.reset': 'earliest'
}
source_consumer = Consumer(source_consumer_conf)
source_consumer.subscribe([SOURCE_TOPIC])

# Producer for local Kafka
producer = Producer(local_kafka_config)

# Consumer for local Kafka
target_consumer_conf = {
    **local_kafka_config,
    'group.id': 'target-consumer-group',
    'auto.offset.reset': 'earliest'
}
target_consumer = Consumer(target_consumer_conf)
target_consumer.subscribe([TARGET_TOPIC])
