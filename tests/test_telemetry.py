
import unittest

from src.producer.telemetry_generator import (
    DEVICES,
    generate_telemetry
)


class TestTelemetry(unittest.TestCase):

    def test_required_fields(self):
        event = generate_telemetry(DEVICES[0])

        required_fields = [
            "device_id",
            "device_type",
            "ip_address",
            "timestamp",
            "latency_ms",
            "packet_loss_pct",
            "bandwidth_mbps",
            "cpu_usage_pct",
            "memory_usage_pct",
            "status"
        ]

        for field in required_fields:
            self.assertIn(field, event)

    def test_device_status(self):
        event = generate_telemetry(DEVICES[0])

        self.assertIn(
            event["status"],
            ["online", "offline"]
        )

    def test_packet_loss_range(self):
        event = generate_telemetry(DEVICES[0])

        self.assertGreaterEqual(
            event["packet_loss_pct"], 0
        )

        self.assertLessEqual(
            event["packet_loss_pct"], 100
        )

    def test_offline_behavior(self):
        # Run enough samples to find an offline event
        offline_event = None

        for _ in range(1000):
            event = generate_telemetry(DEVICES[0])

            if event["status"] == "offline":
                offline_event = event
                break

        self.assertIsNotNone(
            offline_event,
            "No offline event generated"
        )

        self.assertIsNone(
            offline_event["latency_ms"]
        )

        self.assertEqual(
            offline_event["packet_loss_pct"], 100
        )


if __name__ == "__main__":
    unittest.main()
