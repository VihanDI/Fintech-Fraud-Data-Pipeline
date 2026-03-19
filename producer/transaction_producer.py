import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# connecting to Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

countries = ["Sri Lanka", "USA", "UK", "China", "Singapore"]
categories = ["electronics", "food", "travel", "fashion", "gaming"]

def generate_transaction():
    tx = {
        "user_id": random.randint(1000, 1100),
        "timestamp": datetime.utcnow().isoformat(),
        "amount": round(random.uniform(5, 500), 2),
        "country": random.choice(countries),
        "category": random.choices(categories)
    }

    # injecting fraud transaction (with 0.05 probability)
    if random.random() < 0.05:
        tx["amount"] = round(random.uniform(5000, 9000), 2)
        tx["fraud_flag"] = "high_value"

    return tx

print("Starting transaction producer...")

while True:
    transaction = generate_transaction()

    producer.send("transactions", transaction)
    producer.flush()

    print(transaction)

    time.sleep(1)