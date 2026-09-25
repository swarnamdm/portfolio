"""Sample Kafka producer — emits order events to the `events_raw` topic.

Demo / concept code: shows the producer pattern (keyed, idempotent, JSON payload).
Not connected to a real cluster in this demo.
"""
import json
import random
import time
from datetime import datetime, timezone

# from kafka import KafkaProducer  # kafka-python, when a broker is available

TOPIC = "events_raw"


def make_event(order_id: int) -> dict:
    return {
        "event_id": f"evt-{order_id:06d}",
        "order_id": order_id,
        "customer_id": random.randint(1000, 9999),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "region": random.choice(["West", "East", "Central", "South"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main(n_events: int = 100) -> None:
    # producer = KafkaProducer(
    #     bootstrap_servers=["kafka:9092"],
    #     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    #     key_serializer=lambda k: str(k).encode("utf-8"),
    #     enable_idempotence=True,          # exactly-once producer semantics
    #     acks="all",
    # )
    for i in range(1, n_events + 1):
        event = make_event(i)
        # producer.send(TOPIC, key=event["customer_id"], value=event)
        print(json.dumps(event))  # demo: print instead of sending
        time.sleep(0.01)
    # producer.flush()


if __name__ == "__main__":
    main()
