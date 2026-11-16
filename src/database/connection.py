
import os
import psycopg


def get_connection():
    """Create a connection to the NetPulse database."""

    return psycopg.connect(
        dbname=os.getenv("NETPULSE_DB_NAME", "netpulse"),
        user=os.getenv("NETPULSE_DB_USER") or None,
        host=os.getenv("NETPULSE_DB_HOST", "localhost"),
        port=int(os.getenv("NETPULSE_DB_PORT", "5432")),
        password=os.getenv("NETPULSE_DB_PASSWORD") or None,
    )
