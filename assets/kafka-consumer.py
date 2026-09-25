"""Sample Kafka consumer — reads `events_raw`, applies tumbling-window aggregation,
and sinks micro-batches to Snowflake.

Demo / concept code: shows the consumer + windowed-aggregation + sink pattern.
The runnable local simulation is in simulate_streaming.py.
"""
import json
from collections import defaultdict
from datetime import datetime, timezone

# from kafka import KafkaConsumer  # kafka-python
# import snowflake.connector       # snowflake-connector-python

TOPIC = "events_raw"
WINDOW_SECONDS = 60


def window_key(ts: datetime) -> str:
    """Tumbling window: floor event time to the minute."""
    floored = ts.replace(second=0, microsecond=0)
    return floored.isoformat()


def aggregate(events: list[dict]) -> dict:
    windows: dict[str, dict] = defaultdict(lambda: {"count": 0, "revenue": 0.0})
    for e in events:
        ts = datetime.fromisoformat(e["event_time"])
        w = windows[window_key(ts)]
        w["count"] += 1
        w["revenue"] = round(w["revenue"] + e["amount"], 2)
    return dict(windows)


def sink_to_snowflake(rows: list[tuple]) -> None:
    """Micro-batch INSERT into Snowflake (demo: print the SQL instead)."""
    # ctx = snowflake.connector.connect(user=..., account=..., warehouse=...,
    #                                  database="ANALYTICS", schema="STREAMING")
    # cs = ctx.cursor()
    # cs.executemany(
    #     "INSERT INTO events_agg (window_start, event_count, revenue) VALUES (%s,%s,%s)",
    #     rows,
    # )
    for window_start, count, revenue in rows:
        print(f"SINK window={window_start} count={count} revenue={revenue}")


def main() -> None:
    # consumer = KafkaConsumer(TOPIC, bootstrap_servers=["kafka:9092"],
    #                        value_deserializer=lambda m: json.loads(m.decode()))
    # for batch in consumer.poll(timeout_ms=1000).values():
    #     events = [rec.value for recs in batch for rec in recs]
    #     ...
    print("Consumer pattern: poll -> tumbling-window aggregate -> Snowflake sink.")
    print("See simulate_streaming.py for the runnable local version.")


if __name__ == "__main__":
    main()
