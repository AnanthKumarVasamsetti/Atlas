from kafka import KafkaProducer
import json
import time
import uuid
from random import randint, choice
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, EVENTS_PER_SECOND

producer = KafkaProducer(
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

merchants = ["Amazon", "Flipkart", "Walmart", "Target", "Ikea"]
currencies = ["USD", "EUR", "INR"]

def generate_event():
    return {
        "transactionId": str(uuid.uuid4()),
        "userId": randint(10000, 99999),
        "amount": round(randint(100, 50000)/1.3, 2),
        "currency": choice(currencies),
        "merchant": choice(merchants),
        "timestamp": int(time.time())
    }

def start_producing():
    print("[Atlas Producer] starting event production...")

    interval = 1 / EVENTS_PER_SECOND

    while True:
        event = generate_event()
        print(event)
        # producer.send(KAFKA_TOPIC, event)
        time.sleep(interval)

start_producing()