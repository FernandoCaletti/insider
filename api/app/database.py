"""Database connection for the API using psycopg2."""

from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras

from api.app.config import get_settings


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Get a database connection context manager."""
    settings = get_settings()
    conn = psycopg2.connect(settings.database_url)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_cursor(
    cursor_factory: type | None = None,
) -> Generator[psycopg2.extensions.cursor, None, None]:
    """Get a database cursor with auto-commit on success, rollback on error."""
    factory = cursor_factory or psycopg2.extras.RealDictCursor
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=factory)
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
