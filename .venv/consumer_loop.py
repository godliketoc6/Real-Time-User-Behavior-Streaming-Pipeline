import json
import time
from kafka_utils import target_consumer, TARGET_TOPIC
from mongo_utils import collection
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

BATCH_SIZE = 50
BATCH_TIMEOUT = 1  # seconds


def insert_skip_duplicates(docs):
    """Insert docs into MongoDB, skip if _id already exists."""
    if not docs:
        return

    ops = []
    for doc in docs:
        # only insert if new (_id does not exist)
        ops.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {"$setOnInsert": doc},
                upsert=True
            )
        )

    try:
        result = collection.bulk_write(ops, ordered=False)
        inserted = result.upserted_count
        skipped = len(docs) - inserted
        print(f"✅ Inserted {inserted}, ⏭️ Skipped {skipped} duplicates")
    except BulkWriteError as bwe:
        print("⚠️ Bulk write error:", bwe.details)


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
                insert_skip_duplicates(buffer)
                buffer.clear()
                last_flush_time = time.time()

    except KeyboardInterrupt:
        print("\n🛑 Consumer loop interrupted.")
    finally:
        target_consumer.close()
        if buffer:
            insert_skip_duplicates(buffer)
        print("✅ Target consumer closed.")
