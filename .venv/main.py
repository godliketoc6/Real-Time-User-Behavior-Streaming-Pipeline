import json
import time
from kafka_utils import source_consumer, target_consumer, producer, SOURCE_TOPIC, TARGET_TOPIC
from mongo_utils import collection
from confluent_kafka import TopicPartition

def get_end_offsets(consumer, topic):
    """Return dict {partition: end_offset} for topic."""
    partitions = [TopicPartition(topic, p) for p in consumer.list_topics(topic).topics[topic].partitions]
    watermarks = {}
    for tp in partitions:
        low, high = consumer.get_watermark_offsets(tp)
        watermarks[tp.partition] = high
    return watermarks

count = 0

# Find end offsets of remote topic (to know when we're done)
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

        # Check if we have reached end of topic
        tp = TopicPartition(msg.topic(), msg.partition())
        current_offset = msg.offset() + 1  # Kafka offset is zero-based
        if current_offset >= end_offsets.get(msg.partition(), float('inf')):
            print(f"✅ Finished partition {msg.partition()} at offset {current_offset}")
            break

        message_value = msg.value().decode('utf-8')
        print(f"[{count+1}] Pulled from remote '{SOURCE_TOPIC}': {message_value}")

        # Produce to local Kafka
        producer.produce(TARGET_TOPIC, message_value)
        producer.flush()
        print(f"👉 Sent to local topic '{TARGET_TOPIC}'")

        # Consume from local Kafka
        time.sleep(1)
        msg2 = target_consumer.poll(3.0)
        if msg2 is None:
            print("⚠️ No message received from local topic.")
            continue
        if msg2.error():
            print("❌ Local consumer error:", msg2.error())
            continue

        try:
            json_data = json.loads(msg2.value().decode('utf-8'))
            collection.insert_one(json_data)
            print("✅ Inserted into MongoDB")
        except json.JSONDecodeError:
            print("❌ Skipped invalid JSON:", msg2.value())

        count += 1

except KeyboardInterrupt:
    print("\n🛑 Interrupted by user.")
finally:
    source_consumer.close()
    target_consumer.close()
    print("✅ All connections closed.")
