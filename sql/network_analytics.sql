
-- NetPulse Network Analytics
-- Phase 2: PostgreSQL

-- 1. Total telemetry records
SELECT COUNT(*) AS total_telemetry_records
FROM telemetry;


-- 2. Average latency by device
SELECT
    device_id,
    ROUND(AVG(latency_ms)::numeric, 2) AS avg_latency_ms
FROM telemetry
WHERE status = 'online'
GROUP BY device_id
ORDER BY avg_latency_ms DESC;


-- 3. Average packet loss by device
SELECT
    device_id,
    ROUND(AVG(packet_loss_pct)::numeric, 2) AS avg_packet_loss_pct
FROM telemetry
WHERE status = 'online'
GROUP BY device_id
ORDER BY avg_packet_loss_pct DESC;


-- 4. Anomaly frequency
SELECT
    anomaly_type,
    COUNT(*) AS total_occurrences
FROM anomalies
GROUP BY anomaly_type
ORDER BY total_occurrences DESC;


-- 5. Anomalies by device
SELECT
    t.device_id,
    COUNT(a.anomaly_id) AS total_anomalies
FROM telemetry t
JOIN anomalies a
    ON t.telemetry_id = a.telemetry_id
GROUP BY t.device_id
ORDER BY total_anomalies DESC;


-- 6. Device outage frequency
SELECT
    device_id,
    COUNT(*) AS offline_events
FROM telemetry
WHERE status = 'offline'
GROUP BY device_id
ORDER BY offline_events DESC;
