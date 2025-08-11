import os
from dotenv import load_dotenv

load_dotenv()

base_kafka_config = {
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
}

remote_kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_REMOTE_BOOTSTRAP'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    **base_kafka_config,
}

local_kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_LOCAL_BOOTSTRAP'),
    'security.protocol': os.getenv('KAFKA_LOCAL_SECURITY_PROTOCOL'),
    **base_kafka_config,
}

SOURCE_TOPIC = os.getenv("SOURCE_TOPIC")
TARGET_TOPIC = os.getenv("TARGET_TOPIC")

MONGO_URI = os.getenv("MONGO_URI")
