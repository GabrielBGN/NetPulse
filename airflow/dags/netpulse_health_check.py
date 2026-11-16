
from datetime import datetime
import os

import psycopg2

from airflow.sdk import dag, task


def get_database_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "netpulse"),
        user=os.getenv("DB_USER", "netpulse"),
        password=os.environ["DB_PASSWORD"],
    )


@dag(
    dag_id="netpulse_health_check",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["netpulse", "monitoring"],
)
def netpulse_health_check():

    @task
    def check_database():
        with get_database_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM telemetry")
                count = cursor.fetchone()[0]

        print("NetPulse Database Health Check")
        print(f"Total telemetry records: {count}")

        return count

    @task
    def check_network_metrics():
        with get_database_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        ROUND(AVG(latency_ms)::NUMERIC, 2),
                        ROUND(AVG(packet_loss_pct)::NUMERIC, 2)
                    FROM telemetry
                """)
                avg_latency, avg_packet_loss = cursor.fetchone()

        print("NetPulse Network Metrics")
        print(f"Average latency: {avg_latency} ms")
        print(f"Average packet loss: {avg_packet_loss}%")

        return {
            "avg_latency_ms": float(avg_latency) if avg_latency is not None else None,
            "avg_packet_loss_pct": float(avg_packet_loss) if avg_packet_loss is not None else None,
        }

    @task
    def check_anomalies():
        with get_database_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT anomaly_type, COUNT(*)
                    FROM anomalies
                    GROUP BY anomaly_type
                    ORDER BY COUNT(*) DESC
                """)
                results = cursor.fetchall()

        print("NetPulse Anomaly Summary")

        for anomaly_type, count in results:
            print(f"{anomaly_type}: {count}")

        return {
            anomaly_type: count
            for anomaly_type, count in results
        }

    database = check_database()
    metrics = check_network_metrics()
    anomalies = check_anomalies()

    database >> [metrics, anomalies]


netpulse_health_check()
