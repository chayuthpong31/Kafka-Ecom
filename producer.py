from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC_NAME = "raw_events"
 
producer = KafkaProducer(
    bootstrap_servers = BOOTSTRAP_SERVERS,
    key_serializer = lambda k: k.encode("utf-8") if k else None, # Control messages that have same key could send into same partition 
    value_serializer = lambda v: json.dumps(v).encode("utf-8") # Convert value into Byte
)

EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PAY"]

def random_timestamp_last_6_days():
    now = datetime.now(timezone.utc)
    past = now - timedelta(days=6)

    random_seconds = random.uniform(0, (now - past).total_seconds())
    return past + timedelta(seconds=random_seconds)

def generate_event():
    """
    Generate e-commerce event include event_id, customer_id, event_type, amount, currency 
    that have 25% chance missing value.
    """
    is_invalid = random.random() < 0.25 

    customer_id = f"CUST_{random.randint(1,5)}"
    event_type = random.choice(EVENT_TYPES)
    amount = round(random.uniform(10,500),2)
    currency = "USD"

    invalid_field = None
    if is_invalid: 
        invalid_field = random.choice([
            "customer_id",
            "event_type",
            "amount",
            "currency"
        ])

    event = {
        "event_id":str(uuid.uuid4()),
        "customer_id": None if invalid_field == "customer_id" else customer_id,
        "event_type": random.choice(INVALID_EVENT_TYPES) if invalid_field == "event_type" else event_type,
        "amount": random.uniform(-500,-10) if invalid_field == "amount" else amount,
        "currency" : None if invalid_field == "currency" else currency,
        "event_timestamp": random_timestamp_last_6_days().replace(tzinfo=None).isoformat(),
        "is_valid": not is_invalid,
        "invalid_field": invalid_field
    }

    return event["customer_id"], event # key, value

print("Starting Kafka producer...")

# Send Message
while True:
    key, event = generate_event()

    producer.send(
        topic = TOPIC_NAME,
        key = key,
        value = event
    )

    print(f"Produced event | key={key} | valid={event["is_valid"]}")

    time.sleep(1)