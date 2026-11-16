
from datetime import datetime

from airflow.sdk import dag, task

from src.producer.telemetry_generator import (
    DEVICES,
    generate_telemetry,
)
from src.processing.anomaly_detector import detect_anomalies
from src.producer.kafka_producer import create_producer, KAFKA_TOPIC

import json


@dag(
    dag_id="netpulse_telemetry_producer",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["netpulse", "kafka", "telemetry"],
)
def netpulse_telemetry_producer():

    @task
    def generate_and_publish():
        producer = create_producer()

        delivered_count = 0
        delivery_errors = []

        def delivery_callback(error, message):
            nonlocal delivered_count

            if error is not None:
                delivery_errors.append(str(error))
                print(f"Kafka delivery failed: {error}")
            else:
                delivered_count += 1
                print(
                    f"Delivered {message.key().decode('utf-8')} "
                    f"to partition {message.partition()}, "
                    f"offset {message.offset()}"
                )

        print("Starting NetPulse telemetry generation...")

        for device in DEVICES:
            event = generate_telemetry(device)
            event["anomalies"] = detect_anomalies(event)

            producer.produce(
                topic=KAFKA_TOPIC,
                key=event["device_id"],
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_callback,
            )

            producer.poll(0)

            print(f"Queued telemetry for {event['device_id']}")

        remaining = producer.flush(timeout=15)

        if remaining > 0:
            raise RuntimeError(
                f"{remaining} Kafka messages remain undelivered"
            )

        if delivery_errors:
            raise RuntimeError(
                f"{len(delivery_errors)} Kafka deliveries failed: "
                + "; ".join(delivery_errors)
            )

        if delivered_count != len(DEVICES):
            raise RuntimeError(
                f"Expected {len(DEVICES)} deliveries, "
                f"but confirmed {delivered_count}"
            )

        print(
            f"Successfully delivered {delivered_count} "
            "telemetry events to Kafka"
        )

        return delivered_count

    generate_and_publish()


netpulse_telemetry_producer()
