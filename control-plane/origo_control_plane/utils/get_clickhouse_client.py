from clickhouse_connect import get_client

from origo_control_plane.config import resolve_clickhouse_http_settings


def get_clickhouse_client():
    """Returns a ClickHouse client."""

    settings = resolve_clickhouse_http_settings()

    client = get_client(
        host=settings.host,
        port=settings.port,
        username=settings.user,
        password=settings.password,
        database=settings.database,
    )

    return client
