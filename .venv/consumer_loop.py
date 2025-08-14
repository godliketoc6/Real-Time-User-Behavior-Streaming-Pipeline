import json
import time
from kafka_utils import target_consumer, TARGET_TOPIC
from mongo_utils import collection

BATCH_SIZE = 50
BATCH_TIMEOUT = 1  # seconds

def run_consumer_loop():
    buffer = []
    last_flush_time = time.time()

    try:
        while True:
            msg = target_consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("❌ Local consumer error:", msg.error())
                continue

            try:
                json_data = json.loads(msg.value().decode('utf-8'))
                buffer.append(json_data)
            except json.JSONDecodeError:
                print("❌ Skipped invalid JSON:", msg.value())
                continue

            # Flush if batch full or timeout
            if len(buffer) >= BATCH_SIZE or (time.time() - last_flush_time) >= BATCH_TIMEOUT:
                collection.insert_many(buffer)
                print(f"✅ Inserted {len(buffer)} documents into MongoDB")
                buffer.clear()
                last_flush_time = time.time()

    except KeyboardInterrupt:
        print("\n🛑 Consumer loop interrupted.")
    finally:
        target_consumer.close()
        if buffer:
            collection.insert_many(buffer)
            print(f"✅ Inserted remaining {len(buffer)} documents into MongoDB")
        print("✅ Target consumer closed.")
