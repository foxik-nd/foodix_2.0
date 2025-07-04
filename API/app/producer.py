import os
import json
import logging
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "produits_scannes")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

_producer = None

def get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        logger.info(f"Kafka Producer prêt sur {KAFKA_BOOTSTRAP_SERVERS}")
    return _producer

def send_product_to_kafka(data, topic=None):
    producer = get_producer()
    topic = topic or KAFKA_TOPIC
    try:
        producer.send(topic, data)
        producer.flush()
        logger.info("Donnée envoyée à Kafka")
    except Exception as e:
        logger.error(f"Erreur Kafka : {e}") 