
import os

import psycopg


def get_connection():
    """
    Create a PostgreSQL connection using environment variables.

    Defaults target the local Docker PostgreSQL instance.
    """
    return psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5433")),
        dbname=os.getenv("DB_NAME", "netpulse"),
        user=os.getenv("DB_USER", "netpulse"),
        password=os.getenv("DB_PASSWORD", ""),
    )
