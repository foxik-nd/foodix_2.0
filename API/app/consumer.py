# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import os

# --- Configurations ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "produits_scannes")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
HDFS_LOGS_DIR = "/logs"
HDFS_LOGS_PATH = "/logs/user_events.json"

# --- Init HDFS client ---
hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
hdfs_client.makedirs(HDFS_LOGS_DIR)

def append_log_to_hdfs(log):
    try:
        with hdfs_client.read(HDFS_LOGS_PATH) as reader:
            logs = json.load(reader)
    except Exception:
        logs = []
    logs.append(log)
    with hdfs_client.write(HDFS_LOGS_PATH, overwrite=True, encoding='utf-8') as writer:
        json.dump(logs, writer)

# --- Init Kafka consumer ---
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    group_id='hdfs-writer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening to Kafka topic '{}'...".format(KAFKA_TOPIC))

for message in consumer:
    data = message.value
    print("Message reçu :", data)
    # --- Traitement éventuel ici ---
    append_log_to_hdfs(data)
    print("Message stocké dans HDFS.")
