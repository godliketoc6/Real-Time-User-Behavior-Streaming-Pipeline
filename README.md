# Real-Time User Behavior Streaming Pipeline

A small, configurable Python pipeline that **consumes** messages from a remote Kafka cluster, **produces** them into a local Kafka topic, then **consumes** from the local topic and **persists** messages into MongoDB.

Perfect for mirroring data, bridging clusters, or bootstrapping a local development dataset.

---

## Features
- Consume from a remote Kafka topic (supports SASL authentication if needed)
- Produce to a local Kafka topic
- Consume from local Kafka and insert into MongoDB
- Fully configurable via `.env` file
- Separate configuration, Kafka utilities, and MongoDB utilities for maintainability
- Option to stop after fully consuming the remote topic (offset-aware)

---

## Project name
**`kafka-mongodb-data-pipeline`**

---

## Repository layout
```
kafka-mongodb-data-pipeline/
├── config.py # Loads .env and exposes configurations
├── kafka_utils.py # Kafka Producer and Consumer setup
├── mongo_utils.py # MongoDB connection setup
├── main.py # Pipeline execution logic
├── requirements.txt # Python dependencies
├── .env.example # Example environment configuration
└── README.md
```

## Requirements
- Python 3.8+
- Access to a **remote Kafka cluster**
- Access to a **local Kafka broker**
- Access to a **MongoDB** instance

