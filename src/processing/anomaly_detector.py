
LATENCY_THRESHOLD = 80
PACKET_LOSS_THRESHOLD = 3
CPU_THRESHOLD = 85
MEMORY_THRESHOLD = 85


def detect_anomalies(event):
    anomalies = []

    if event.get("status") == "offline":
        anomalies.append("DEVICE_OFFLINE")
        return anomalies

    latency = event.get("latency_ms")
    packet_loss = event.get("packet_loss_pct")
    cpu = event.get("cpu_usage_pct")
    memory = event.get("memory_usage_pct")

    if latency is not None and latency > LATENCY_THRESHOLD:
        anomalies.append("HIGH_LATENCY")

    if packet_loss is not None and packet_loss > PACKET_LOSS_THRESHOLD:
        anomalies.append("HIGH_PACKET_LOSS")

    if cpu is not None and cpu > CPU_THRESHOLD:
        anomalies.append("HIGH_CPU_USAGE")

    if memory is not None and memory > MEMORY_THRESHOLD:
        anomalies.append("HIGH_MEMORY_USAGE")

    return anomalies
