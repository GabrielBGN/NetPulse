
import json
from confluent_kafka import Producer

KAFKA_TOPIC = "network-telemetry"


def delivery_report(error, message):
    """Report the result of Kafka message delivery."""
    if error is not None:
        print(f"Kafka delivery failed: {error}")
    else:
        print(
            f"Delivered to {message.topic()} "
            f"[partition {message.partition()}] "
            f"offset {message.offset()}"
        )


def create_producer():
    """Connect to our local Kafka broker."""
    return Producer({
        "bootstrap.servers": "127.0.0.1:9092",
        "client.id": "netpulse-producer",
    })


def publish_telemetry(producer, event):
    """Publish a telemetry event to Kafka."""
    producer.produce(
        topic=KAFKA_TOPIC,
        key=event["device_id"],
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )

    producer.poll(0)
