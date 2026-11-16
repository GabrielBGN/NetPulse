
import json

from confluent_kafka import Consumer, KafkaException

from src.database.ingest import insert_telemetry


KAFKA_TOPIC = "network-telemetry"


def create_consumer():
    """Create a consumer connected to the Kafka broker."""
    return Consumer({
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": "netpulse-db-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def consume_telemetry():
    """Read Kafka events and insert them into PostgreSQL."""
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    print("Waiting for Kafka telemetry events...")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            try:
                event = json.loads(message.value().decode("utf-8"))

                telemetry_id = insert_telemetry(event)

                # Commit only after PostgreSQL insertion succeeds.
                consumer.commit(message=message, asynchronous=False)

                print(
                    f"Saved {event['device_id']} to PostgreSQL "
                    f"(telemetry_id={telemetry_id})"
                )

            except Exception as error:
                print(f"Failed to process Kafka message: {error}")
                # Stop instead of skipping an uncommitted message.
                raise

    except KeyboardInterrupt:
        print("\nStopping Kafka consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    consume_telemetry()
