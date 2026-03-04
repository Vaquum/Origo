import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from clickhouse_driver import Client as ClickHouseClient

from .config import MigrationSettings

FILE_PATTERN = re.compile(r'^(\d{4,})__([a-z0-9_]+)\.sql$')
IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
PLACEHOLDER_PATTERN = re.compile(r'\{\{([A-Z_]+)\}\}')


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path
    checksum: str
    sql: str


@dataclass(frozen=True)
class MigrationStatus:
    version: int
    name: str
    checksum: str
    state: str
    applied_at: datetime | None


class MigrationRunner:
    def __init__(
        self, settings: MigrationSettings, migrations_dir: Path | None = None
    ) -> None:
        self._settings = settings
        self._migrations_dir = (
            migrations_dir
            if migrations_dir is not None
            else Path(__file__).resolve().parents[2] / 'migrations' / 'sql'
        )

        if not IDENTIFIER_PATTERN.match(self._settings.database):
            raise RuntimeError(
                f'Invalid CLICKHOUSE_DATABASE identifier: {self._settings.database}'
            )

        self._client = ClickHouseClient(
            host=self._settings.host,
            port=self._settings.port,
            user=self._settings.user,
            password=self._settings.password,
        )

    def close(self) -> None:
        self._client.disconnect()

    def migrate(self) -> list[MigrationStatus]:
        statuses = self.status()
        pending_versions = [s.version for s in statuses if s.state == 'pending']
        if not pending_versions:
            return []

        migrations = self._load_migrations()
        migrations_by_version = {
            migration.version: migration for migration in migrations
        }
        applied: list[MigrationStatus] = []

        for version in pending_versions:
            migration = migrations_by_version[version]
            self._client.execute(migration.sql)
            self._client.execute(
                f"""
                INSERT INTO {self._settings.database}.schema_migrations
                (version, name, checksum, applied_at)
                VALUES
                """,
                [
                    (
                        migration.version,
                        migration.name,
                        migration.checksum,
                        datetime.now(UTC),
                    )
                ],
            )
            applied.append(
                MigrationStatus(
                    version=migration.version,
                    name=migration.name,
                    checksum=migration.checksum,
                    state='applied',
                    applied_at=datetime.now(UTC),
                )
            )

        return applied

    def status(self) -> list[MigrationStatus]:
        self._ensure_database_exists()
        self._ensure_ledger_exists()

        migrations = self._load_migrations()
        applied_index = self._fetch_applied_index()
        statuses: list[MigrationStatus] = []

        migration_versions = {migration.version for migration in migrations}
        for applied_version in applied_index:
            if applied_version not in migration_versions:
                raise RuntimeError(
                    f'Applied migration {applied_version} is missing from migrations directory'
                )

        for migration in migrations:
            applied = applied_index.get(migration.version)
            if applied is None:
                statuses.append(
                    MigrationStatus(
                        version=migration.version,
                        name=migration.name,
                        checksum=migration.checksum,
                        state='pending',
                        applied_at=None,
                    )
                )
                continue

            applied_name, applied_checksum, applied_at = applied
            if applied_name != migration.name:
                raise RuntimeError(
                    f'Migration name mismatch for version {migration.version}: '
                    f'db={applied_name} file={migration.name}'
                )
            if applied_checksum != migration.checksum:
                raise RuntimeError(
                    f'Checksum mismatch for version {migration.version}: '
                    f'db={applied_checksum} file={migration.checksum}'
                )

            statuses.append(
                MigrationStatus(
                    version=migration.version,
                    name=migration.name,
                    checksum=migration.checksum,
                    state='applied',
                    applied_at=applied_at,
                )
            )

        return statuses

    def _ensure_database_exists(self) -> None:
        self._client.execute(
            f"""
            CREATE DATABASE IF NOT EXISTS {self._settings.database}
            ENGINE = Atomic
            """
        )

    def _ensure_ledger_exists(self) -> None:
        self._client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._settings.database}.schema_migrations (
                version UInt32,
                name String,
                checksum String,
                applied_at DateTime
            )
            ENGINE = MergeTree()
            ORDER BY (version)
            """
        )

    def _fetch_applied_index(self) -> dict[int, tuple[str, str, datetime]]:
        rows = self._client.execute(
            f"""
            SELECT version, name, checksum, applied_at
            FROM {self._settings.database}.schema_migrations
            ORDER BY version ASC
            """
        )

        applied: dict[int, tuple[str, str, datetime]] = {}
        for version, name, checksum, applied_at in rows:
            if version in applied:
                raise RuntimeError(
                    f'Duplicate version found in schema_migrations: {version}'
                )
            applied[version] = (name, checksum, applied_at)
        return applied

    def _load_migrations(self) -> list[Migration]:
        if not self._migrations_dir.exists():
            raise RuntimeError(
                f'Migrations directory does not exist: {self._migrations_dir}'
            )

        files = sorted(self._migrations_dir.glob('*.sql'))
        migrations: list[Migration] = []

        for migration_file in files:
            version, name = self._parse_filename(migration_file.name)
            raw_sql = migration_file.read_text(encoding='utf-8')
            checksum = hashlib.sha256(raw_sql.encode('utf-8')).hexdigest()
            rendered_sql = self._render_sql(raw_sql)
            normalized_sql = self._normalize_single_statement(
                rendered_sql, migration_file
            )

            migrations.append(
                Migration(
                    version=version,
                    name=name,
                    path=migration_file,
                    checksum=checksum,
                    sql=normalized_sql,
                )
            )

        if not migrations:
            return migrations

        versions = [migration.version for migration in migrations]
        if len(versions) != len(set(versions)):
            raise RuntimeError('Duplicate migration versions detected')

        expected_versions = list(range(1, len(versions) + 1))
        if versions != expected_versions:
            raise RuntimeError(
                f'Migration versions must be contiguous and start at 0001. '
                f'Found versions={versions}'
            )

        return migrations

    @staticmethod
    def _parse_filename(filename: str) -> tuple[int, str]:
        match = FILE_PATTERN.match(filename)
        if not match:
            raise RuntimeError(
                f'Invalid migration filename: {filename}. Expected NNNN__name.sql'
            )

        version = int(match.group(1))
        name = match.group(2)
        if version <= 0:
            raise RuntimeError(f'Migration version must be positive: {filename}')
        return version, name

    def _render_sql(self, raw_sql: str) -> str:
        rendered = raw_sql.replace('{{DATABASE}}', self._settings.database)
        unresolved = PLACEHOLDER_PATTERN.findall(rendered)
        if unresolved:
            raise RuntimeError(
                f'Unresolved SQL placeholder(s): {sorted(set(unresolved))}'
            )
        return rendered

    @staticmethod
    def _normalize_single_statement(sql: str, migration_file: Path) -> str:
        statement = sql.strip()
        if not statement:
            raise RuntimeError(f'Migration file is empty: {migration_file}')

        semicolon_count = statement.count(';')
        if semicolon_count > 1 or (
            semicolon_count == 1 and not statement.endswith(';')
        ):
            raise RuntimeError(
                f'Migration files must contain exactly one SQL statement: {migration_file}'
            )

        if statement.endswith(';'):
            statement = statement[:-1].strip()

        if not statement:
            raise RuntimeError(
                f'Migration file has no executable SQL: {migration_file}'
            )

        return statement
