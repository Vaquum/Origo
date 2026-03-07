from dataclasses import dataclass

from origo_control_plane.config import resolve_clickhouse_native_settings


@dataclass(frozen=True)
class MigrationSettings:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls) -> 'MigrationSettings':
        settings = resolve_clickhouse_native_settings()

        return cls(
            host=settings.host,
            port=settings.port,
            user=settings.user,
            password=settings.password,
            database=settings.database,
        )
