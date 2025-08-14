import threading
from producer_loop import run_producer_loop
from consumer_loop import run_consumer_loop

if __name__ == "__main__":
    producer_thread = threading.Thread(target=run_producer_loop, daemon=True)
    consumer_thread = threading.Thread(target=run_consumer_loop, daemon=True)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()
