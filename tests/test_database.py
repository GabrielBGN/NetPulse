
import unittest

from src.database.connection import get_connection
from src.database.ingest import insert_telemetry
from src.producer.telemetry_generator import (
    DEVICES,
    generate_telemetry,
)
from src.processing.anomaly_detector import detect_anomalies


class TestDatabaseIngestion(unittest.TestCase):

    def test_insert_telemetry(self):
        event = generate_telemetry(DEVICES[0])
        event["anomalies"] = detect_anomalies(event)

        telemetry_id = insert_telemetry(event)

        self.assertIsInstance(telemetry_id, int)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT device_id
                    FROM telemetry
                    WHERE telemetry_id = %s
                    """,
                    (telemetry_id,),
                )

                result = cur.fetchone()

                self.assertIsNotNone(result)
                self.assertEqual(result[0], "router-01")

                # Remove the test record
                cur.execute(
                    "DELETE FROM telemetry WHERE telemetry_id = %s",
                    (telemetry_id,),
                )

    def test_anomaly_insertion(self):
        event = generate_telemetry(DEVICES[0])

        # Force a known anomaly
        event["status"] = "online"
        event["latency_ms"] = 95.0
        event["anomalies"] = ["HIGH_LATENCY"]

        telemetry_id = insert_telemetry(event)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT anomaly_type
                    FROM anomalies
                    WHERE telemetry_id = %s
                    """,
                    (telemetry_id,),
                )

                anomalies = [
                    row[0] for row in cur.fetchall()
                ]

                self.assertIn("HIGH_LATENCY", anomalies)

                # Cascade-delete the related anomaly
                cur.execute(
                    "DELETE FROM telemetry WHERE telemetry_id = %s",
                    (telemetry_id,),
                )


if __name__ == "__main__":
    unittest.main()
