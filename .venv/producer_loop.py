import time
import signal
from confluent_kafka import KafkaError
from kafka_utils import source_consumer, producer, SOURCE_TOPIC, TARGET_TOPIC

# ---- Configurable knobs ----
POLL_TIMEOUT_SECS = 1.0          # consumer poll timeout
COMMIT_EVERY_N_MESSAGES = 1000   # commit offsets every N messages
COMMIT_EVERY_SECS = 2            # ...or at least every T seconds
DELIVERY_TIMEOUT_SECS = 10       # final producer flush timeout

_running = True


def _handle_sigterm(signum, frame):
    """Allow Ctrl+C / SIGTERM to exit the loop cleanly."""
    global _running
    _running = False


# Register graceful shutdown handlers
signal.signal(signal.SIGINT, _handle_sigterm)
signal.signal(signal.SIGTERM, _handle_sigterm)


def _on_delivery(err, msg):
    """Delivery report callback for debugging/monitoring."""
    if err is not None:
        # You may want to log this to a file/monitoring system
        print(f"❌ Delivery failed to '{msg.topic()}': {err}")
    # else:
    #     print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def run_producer_loop():
    """
    24/7 pipe: read from SOURCE_TOPIC (remote) and forward to TARGET_TOPIC (local).
    - No snapshot of end offsets (continuous streaming).
    - Periodic offset commits to resume correctly after restarts.
    - Backpressure-safe produce with poll().
    """
    print(f"🚀 Starting 24/7 forwarder: {SOURCE_TOPIC} -> {TARGET_TOPIC}")

    msg_count = 0
    last_commit_ts = time.time()

    try:
        while _running:
            msg = source_consumer.poll(POLL_TIMEOUT_SECS)

            if msg is None:
                # Let the producer make progress (delivery callbacks)
                producer.poll(0)

                # Time-based commit even if idle
                now = time.time()
                if now - last_commit_ts >= COMMIT_EVERY_SECS:
                    try:
                        source_consumer.commit(asynchronous=True)
                        last_commit_ts = now
                        # print("📝 Periodic idle commit")
                    except Exception as e:
                        print(f"⚠️ Commit (idle) failed: {e}")
                continue

            if msg.error():
                # Handle partition EOF if enabled (enable.partition.eof=True), else log and continue
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached logical end of a partition; in 24/7 mode just continue
                    continue
                print(f"❌ Source consumer error: {msg.error()}")
                continue

            # Forward payload as-is (bytes). This avoids decode errors on non-UTF8.
            value_bytes = msg.value()
            key_bytes = msg.key()
            headers = msg.headers()

            # Try to produce; handle temporary queue full with a brief poll
            while _running:
                try:
                    producer.produce(
                        topic=TARGET_TOPIC,
                        value=value_bytes,
                        key=key_bytes,          # keeps ordering semantics per key on the target
                        headers=headers,
                        on_delivery=_on_delivery
                    )
                    break
                except BufferError:
                    # Local queue is full, give the producer time to drain
                    producer.poll(0.5)

            # Let producer run delivery callbacks and free space
            producer.poll(0)

            msg_count += 1

            # Periodic offset commit (asynchronous) for safe resume
            if (msg_count % COMMIT_EVERY_N_MESSAGES) == 0:
                try:
                    source_consumer.commit(asynchronous=True)
                    last_commit_ts = time.time()
                    # print(f"📝 Committed after {msg_count} messages")
                except Exception as e:
                    print(f"⚠️ Commit (count-based) failed: {e}")

            # Optional: lightweight logging
            # if (msg_count % 10000) == 0:
            #     print(f"➡️  Forwarded {msg_count} messages so far...")

    except Exception as e:
        print(f"💥 Unhandled exception in loop: {e}")

    finally:
        # Final commit to persist our position
        try:
            source_consumer.commit()
        except Exception as e:
            print(f"⚠️ Final commit failed: {e}")

        print("🧹 Flushing producer...")
        try:
            producer.flush(DELIVERY_TIMEOUT_SECS)
        except Exception as e:
            print(f"⚠️ Flush failed: {e}")

        source_consumer.close()
        print("✅ Source consumer closed. Bye.")

