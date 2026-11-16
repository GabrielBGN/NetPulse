
-- NetPulse simulated network devices
-- Seed records during initial PostgreSQL setup.

INSERT INTO public.devices (
    device_id,
    device_type,
    ip_address
)
VALUES
    ('router-01', 'router', '192.168.1.1'),
    ('router-02', 'router', '192.168.1.2'),
    ('switch-01', 'switch', '192.168.1.10'),
    ('server-01', 'server', '192.168.1.100')
ON CONFLICT (device_id) DO NOTHING;
