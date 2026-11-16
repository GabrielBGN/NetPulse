
import unittest

from src.processing.anomaly_detector import detect_anomalies


class TestAnomalyDetector(unittest.TestCase):

    def test_healthy_device(self):
        event = {
            "status": "online",
            "latency_ms": 20,
            "packet_loss_pct": 0.1,
            "cpu_usage_pct": 30,
            "memory_usage_pct": 40
        }

        self.assertEqual(detect_anomalies(event), [])

    def test_high_latency(self):
        event = {
            "status": "online",
            "latency_ms": 95
        }

        self.assertIn(
            "HIGH_LATENCY",
            detect_anomalies(event)
        )

    def test_multiple_anomalies(self):
        event = {
            "status": "online",
            "latency_ms": 95,
            "packet_loss_pct": 4.5,
            "cpu_usage_pct": 92,
            "memory_usage_pct": 90
        }

        self.assertEqual(
            len(detect_anomalies(event)), 4
        )

    def test_offline_device(self):
        event = {
            "status": "offline",
            "latency_ms": None,
            "packet_loss_pct": 100
        }

        self.assertEqual(
            detect_anomalies(event),
            ["DEVICE_OFFLINE"]
        )


if __name__ == "__main__":
    unittest.main()
