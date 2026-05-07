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

user_home_country = {}

def generate_transaction():
    user_id = random.randint(1000, 1100)

    # assign home country for user ids
    if user_id not in user_home_country:
        user_home_country[user_id] = random.choice(countries)

    country = user_home_country[user_id]

    tx = {
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "merchant_category": random.choices(categories),
        "amount": round(random.uniform(5, 500), 2),
        "location": country
    }

    # Injecting fraud transactions
    if random.random() < 0.05:
        fraud_type = random.choice(["high_value", "impossible_location"])

        if fraud_type == "high_value":
            tx["amount"] = round(random.uniform(5000, 9000), 2)

        elif fraud_type == "impossible_location":
            other_countries = [c for c in countries if c != country]
            tx["location"] = random.choice(other_countries)

    return tx

print("Starting transaction producer...")

while True:
    transaction = generate_transaction()

    producer.send("transactions", transaction)
    producer.flush()

    print(transaction)

    time.sleep(1)