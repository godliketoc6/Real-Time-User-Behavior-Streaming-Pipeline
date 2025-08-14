import json
import time
from kafka_utils import source_consumer, producer, SOURCE_TOPIC, TARGET_TOPIC
from confluent_kafka import TopicPartition

def get_end_offsets(consumer, topic):
    """Return dict {partition: end_offset} for topic."""
    partitions = [TopicPartition(topic, p) for p in consumer.list_topics(topic).topics[topic].partitions]
    watermarks = {}
    for tp in partitions:
        low, high = consumer.get_watermark_offsets(tp)
        watermarks[tp.partition] = high
    return watermarks

def run_producer_loop():
    count = 0
    end_offsets = get_end_offsets(source_consumer, SOURCE_TOPIC)
    print(f"📦 End offsets for {SOURCE_TOPIC}: {end_offsets}")

    try:
        while True:
            msg = source_consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("❌ Source consumer error:", msg.error())
                continue

            tp = TopicPartition(msg.topic(), msg.partition())
            current_offset = msg.offset() + 1
            if current_offset >= end_offsets.get(msg.partition(), float('inf')):
                print(f"✅ Finished partition {msg.partition()} at offset {current_offset}")
                break

            message_value = msg.value().decode('utf-8')
            print(f"[{count+1}] Pulled from remote '{SOURCE_TOPIC}': {message_value}")

            producer.produce(TARGET_TOPIC, message_value)
            producer.flush()
            print(f"👉 Sent to local topic '{TARGET_TOPIC}'")
            count += 1

    except KeyboardInterrupt:
        print("\n🛑 Producer loop interrupted.")
    finally:
        source_consumer.close()
        print("✅ Source consumer closed.")
