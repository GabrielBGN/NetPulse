import json
import random
import time
from pathlib import Path
from src.processing.anomaly_detector import detect_anomalies
from datetime import datetime, timezone
from src.producer.kafka_producer import create_producer, publish_telemetry

DEVICES = [
    {
        "device_id": "router-01",
        "device_type": "router",
        "ip_address": "192.168.1.1"
    },
    {
        "device_id": "router-02",
        "device_type": "router",
        "ip_address": "192.168.1.2"
    },
    {
        "device_id": "switch-01",
        "device_type": "switch",
        "ip_address": "192.168.1.10"
    },
    {
        "device_id": "server-01",
        "device_type": "server",
        "ip_address": "192.168.1.100"
    }
]


def generate_telemetry(device):

    # Simulate device availability
    status = random.choices(
        ["online", "offline"],
        weights=[98, 2],
        k=1
    )[0]

    if status == "offline":
        latency = None
        packet_loss = 100.0
        bandwidth = 0.0
        cpu_usage = None
        memory_usage = None

    else:
        latency = round(random.uniform(5, 100), 2)
        packet_loss = round(random.uniform(0, 5), 2)
        bandwidth = round(random.uniform(50, 1000), 2)
        cpu_usage = round(random.uniform(10, 95), 2)
        memory_usage = round(random.uniform(20, 90), 2)

    telemetry = {
        "device_id": device["device_id"],
        "device_type": device["device_type"],
        "ip_address": device["ip_address"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latency_ms": latency,
        "packet_loss_pct": packet_loss,
        "bandwidth_mbps": bandwidth,
        "cpu_usage_pct": cpu_usage,
        "memory_usage_pct": memory_usage,
        "status": status
    }

    return telemetry





if __name__ == "__main__":

    output_path = Path("data/telemetry.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Create Kafka producer once
    producer = create_producer()

    print("NetPulse Telemetry Generator Started")
    print("Saving telemetry to data/telemetry.jsonl")
    print("Publishing telemetry to Kafka: network-telemetry")
    print("Press Ctrl+C to stop.\n")

    try:
        with output_path.open("a") as file:

            while True:
                for device in DEVICES:

                    # Generate network telemetry
                    telemetry = generate_telemetry(device)

                    # Detect network anomalies
                    telemetry["anomalies"] = detect_anomalies(telemetry)

                    # Convert telemetry to JSON
                    json_event = json.dumps(telemetry)

                    # Save telemetry to JSONL
                    print(json_event)
                    file.write(json_event + "\n")
                    file.flush()

                    # Publish telemetry to Kafka
                    try:
                        publish_telemetry(producer, telemetry)
                        print(
                            f"Queued for Kafka: {telemetry['device_id']}"
                        )

                    except Exception as error:
                        print(f"Kafka publishing failed: {error}")

                # Generate another batch every 5 seconds
                time.sleep(5)

    except KeyboardInterrupt:
        print("\nNetPulse Telemetry Generator Stopped")

    finally:
        # Wait for pending Kafka messages to finish sending
        print("Flushing pending Kafka messages...")
        remaining = producer.flush(timeout=10)

        print(f"Kafka messages still pending: {remaining}")
        print("Producer shutdown complete.")
