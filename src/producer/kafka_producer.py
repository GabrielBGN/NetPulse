
import json
import os

from confluent_kafka import Producer

KAFKA_TOPIC = "network-telemetry"


def delivery_report(error, message):
    if error is not None:
        print(f"Kafka delivery failed: {error}")
    else:
        print(
            f"Delivered to {message.topic()} "
            f"[partition {message.partition()}] "
            f"offset {message.offset()}"
        )


def create_producer():
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "127.0.0.1:9092",
    )

    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "netpulse-producer",
    })


def publish_telemetry(producer, event):
    producer.produce(
        topic=KAFKA_TOPIC,
        key=event["device_id"],
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)
