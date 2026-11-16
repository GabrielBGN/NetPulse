
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    avg,
    count,
    sum as spark_sum,
    when,
    size,
    round as spark_round,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    ArrayType,
)


# ============================================================
# Kafka Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "network-telemetry"

# Spark checkpoint directory
CHECKPOINT_DIR = "data/checkpoints/spark_analytics"


# ============================================================
# Telemetry Schema
# ============================================================

TELEMETRY_SCHEMA = StructType([
    StructField("device_id", StringType()),
    StructField("device_type", StringType()),
    StructField("ip_address", StringType()),
    StructField("timestamp", StringType()),
    StructField("latency_ms", DoubleType()),
    StructField("packet_loss_pct", DoubleType()),
    StructField("bandwidth_mbps", DoubleType()),
    StructField("cpu_usage_pct", DoubleType()),
    StructField("memory_usage_pct", DoubleType()),
    StructField("status", StringType()),
    StructField("anomalies", ArrayType(StringType())),
])


# ============================================================
# Create Spark Session
# ============================================================

def create_spark_session():
    """Create a local Spark session with Kafka support."""

    return (
        SparkSession.builder
        .appName("NetPulse-Streaming-Analytics")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"
        )
        .config("spark.sql.shuffle.partitions", "4")
        
        .getOrCreate()
    )


# ============================================================
# Parse Kafka Telemetry
# ============================================================

def parse_telemetry(kafka_df):
    """Convert Kafka JSON messages into structured columns."""

    telemetry_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS telemetry_json")
        .select(
            from_json(
                col("telemetry_json"),
                TELEMETRY_SCHEMA
            ).alias("telemetry")
        )
        .select("telemetry.*")
        .filter(col("device_id").isNotNull())
    )

    return telemetry_df


# ============================================================
# Calculate Streaming Analytics
# ============================================================

def calculate_analytics(telemetry_df):
    """Calculate running network statistics for each device."""

    analytics_df = (
        telemetry_df
        .groupBy("device_id")
        .agg(
            # Total telemetry events
            count("*").alias("total_events"),

            # Average network latency
            spark_round(
                avg("latency_ms"), 2
            ).alias("avg_latency_ms"),

            # Average packet loss percentage
            spark_round(
                avg("packet_loss_pct"), 2
            ).alias("avg_packet_loss_pct"),

            # Number of events containing anomalies
            spark_sum(
                when(
                    size(col("anomalies")) > 0, 1
                ).otherwise(0)
            ).alias("events_with_anomalies"),
        )
    )

    return analytics_df


# ============================================================
# Main Streaming Application
# ============================================================

def main():
    """Read Kafka telemetry and calculate real-time analytics."""

    # Create the checkpoint directory if it doesn't exist
    Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("\nNetPulse Spark Streaming Analytics Started")
    print(f"Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Checkpoint Directory: {CHECKPOINT_DIR}")
    print("Press Ctrl+C to stop.\n")

    query = None

    try:
        # Read incoming messages from Kafka
        kafka_df = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                KAFKA_BOOTSTRAP_SERVERS
            )
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )

        # Parse JSON into structured telemetry
        telemetry_df = parse_telemetry(kafka_df)

        # Calculate real-time network statistics
        analytics_df = calculate_analytics(telemetry_df)

        # Display updated analytics after each micro-batch
        query = (
            analytics_df.writeStream
            .format("console")
            .outputMode("complete")
            .option("truncate", "false")
            .option(
                "checkpointLocation",
                CHECKPOINT_DIR
            )
            .start()
        )

        # Keep processing incoming Kafka messages
        query.awaitTermination()

    except KeyboardInterrupt:
        print("\nStopping NetPulse Spark Streaming...")

    finally:
        if query is not None and query.isActive:
            query.stop()

        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()
