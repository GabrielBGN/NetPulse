
from src.database.connection import get_connection


def insert_telemetry(event):
    """Insert a telemetry event and its anomalies."""

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO telemetry (
                    device_id,
                    timestamp,
                    latency_ms,
                    packet_loss_pct,
                    bandwidth_mbps,
                    cpu_usage_pct,
                    memory_usage_pct,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING telemetry_id;
                """,
                (
                    event["device_id"],
                    event["timestamp"],
                    event["latency_ms"],
                    event["packet_loss_pct"],
                    event["bandwidth_mbps"],
                    event["cpu_usage_pct"],
                    event["memory_usage_pct"],
                    event["status"],
                ),
            )

            telemetry_id = cur.fetchone()[0]

            for anomaly in event.get("anomalies", []):
                cur.execute(
                    """
                    INSERT INTO anomalies (
                        telemetry_id,
                        anomaly_type
                    )
                    VALUES (%s, %s);
                    """,
                    (telemetry_id, anomaly),
                )

    return telemetry_id
