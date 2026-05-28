from kafka import KafkaConsumer, KafkaProducer
import json

BOOTSTRAP_SERVERS = "localhost:29092"
INPUT_TOPIC = "raw_events"
OUTPUT_TOPIC = "cleaned_events"
GROUP_ID = "silver-stream-processor"

VALID_EVENTS_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers = BOOTSTRAP_SERVERS,
    group_id = GROUP_ID,
    auto_offset_reset = "earliest",
    enable_auto_commit = False,
    key_deserializer = lambda k : k.decode("utf-8") if k else None,
    value_deserializer = lambda v : json.loads(v.decode("utf-8"))
)
 
producer = KafkaProducer(
    bootstrap_servers = BOOTSTRAP_SERVERS,
    key_serializer = lambda k: k.encode("utf-8") if k else None, 
    value_serializer = lambda v: json.dumps(v).encode("utf-8") 
)

def is_valid_event(event):
    """
    Check is the event valid
    """
    if not event["customer_id"]:
        return False
    if event["event_type"] not in VALID_EVENTS_TYPES:
        return False
    if event["amount"] is None or event["amount"] <= 0:
        return False
    if not event["currency"]:
        return False
    if not event["is_valid"]:
        return False
    return True

print("Starting Silver Stream Processor...")

for message in consumer:
    key = message.key
    event = message.value

    if is_valid_event(event):
        producer.send(
            topic = OUTPUT_TOPIC,
            key = key,
            value = event
        )
        print(f"FORWARDED | key={key} | event_type={event["event_type"]}")
    else:
        print(f"DROPPED | key={key} | reason=invalid")

    consumer.commit()