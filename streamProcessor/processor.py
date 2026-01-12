from kafka import KafkaConsumer, KafkaProducer
import json
import time
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    RAW_TOPIC,
    PROCESSED_TOPIC,
    CONSUMER_GROUP
)

def enrichEvent(event):
    amount = event.get("amount", 0)
    
    isSuspicious = amount > 5000
    riskScore = min(amount / 1000, 10)

    event["isSuspicious"] = isSuspicious
    event["riskScore"] = riskScore

    return event

def start_stream_processor():
    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        group_id = CONSUMER_GROUP,
        auto_offset_reset = "earliest",
        enable_auto_commit = True,
        value_deserializer = lambda m: json.loads(m.decode("utf-8"))
    )

    producer = KafkaProducer(
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        value_serializer = lambda v: json.dumps(v).encode("utf-8")
    )

    print("[ATLAS Processor] Started stream processor...")

    for msg in consumer:
        rawEvent = msg.value
        enriched = enrichEvent(rawEvent)
        producer.send(PROCESSED_TOPIC, enriched)

        print(f"[ATLAS Processor] Processed event {enriched['TransactionId']}")