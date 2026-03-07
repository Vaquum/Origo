from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, asset

from origo_control_plane.config import resolve_clickhouse_native_settings

_CLICKHOUSE = resolve_clickhouse_native_settings()
CLICKHOUSE_HOST = _CLICKHOUSE.host
CLICKHOUSE_PORT = _CLICKHOUSE.port
CLICKHOUSE_USER = _CLICKHOUSE.user
CLICKHOUSE_PASSWORD = _CLICKHOUSE.password
CLICKHOUSE_DATABASE = _CLICKHOUSE.database


@asset(
    group_name='origo_setup',
    description='Creates the ORIGO database with optimal settings for exchange data',
)
def create_origo_database(context: AssetExecutionContext):
    """
    Creates a ClickHouse database optimized for high-performance exchange data analytics.
    """
    client = None
    try:
        # Connect to ClickHouse
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )

        # Check if database exists
        result = client.execute(f"SHOW DATABASES LIKE '{CLICKHOUSE_DATABASE}'")
        if not result:
            # Create the database with optimal settings
            client.execute(f"""
                CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}
                ENGINE = Atomic
            """)

            # Set optimal database-level settings for exchange data workloads
            settings = [
                # Storage optimization
                (
                    'max_partitions_per_insert_block',
                    '1000',
                ),  # Allow many partitions in a single insert
                # Query performance
                (
                    'max_threads',
                    '16',
                ),  # Adjust based on server CPU cores for query parallelism
                ('max_memory_usage', '20000000000'),  # 20GB max memory per query
                # Optimizations for time-series data
                (
                    'allow_experimental_lightweight_delete',
                    '1',
                ),  # Enable lightweight deletes for time-series data
                ('max_insert_block_size', '1048576'),  # Larger blocks for bulk inserts
                (
                    'min_insert_block_size_rows',
                    '1000000',
                ),  # Minimum size for insert blocks
                ('min_insert_block_size_bytes', '100000000'),  # 100MB minimum size
                # Query optimization
                ('optimize_on_insert', '1'),  # Optimize data on insert
                ('enable_unaligned_array_join', '1'),  # Better array join performance
                # Disable expensive operations
                (
                    'allow_suspicious_low_cardinality_types',
                    '0',
                ),  # Avoid inefficient column types
                (
                    'optimize_skip_unused_shards',
                    '1',
                ),  # Skip shards when possible in distributed queries
                # Cache settings
                ('mark_cache_size', '5368709120'),  # 5GB mark cache for faster reads
                ('use_query_cache', '1'),  # Enable query cache
            ]

            # Apply settings at the database level where possible
            for setting, value in settings:
                try:
                    client.execute(f"""
                        ALTER DATABASE {CLICKHOUSE_DATABASE} 
                        MODIFY SETTING {setting} = {value}
                    """)
                except Exception as e:
                    # Some settings may not be applicable at database level
                    context.log.warning(
                        f'Could not set {setting}={value} at database level: {e!s}'
                    )

            return {'database_created': True, 'database_name': CLICKHOUSE_DATABASE}
        else:
            return {
                'database_created': False,
                'database_name': CLICKHOUSE_DATABASE,
                'message': 'Database already exists',
            }

    finally:
        # Ensure client is disconnected
        if client:
            try:
                client.disconnect()
            except Exception as e:
                context.log.warning(f'Error disconnecting from ClickHouse: {e!s}')
