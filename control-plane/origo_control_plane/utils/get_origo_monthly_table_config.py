import re

_IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def get_origo_monthly_table_config(id_col: str) -> str:
    if not _IDENTIFIER_PATTERN.match(id_col):
        raise ValueError(f'Invalid SQL identifier for id_col: {id_col}')

    db_config = f"""ENGINE = MergeTree()
                    PARTITION BY toYYYYMM(datetime)
                    ORDER BY (toStartOfDay(datetime), {id_col})
                    SAMPLE BY {id_col}
                    SETTINGS 
                        index_granularity = 8192,
                        enable_mixed_granularity_parts = 1,
                        min_rows_for_wide_part = 1000000,
                        min_bytes_for_wide_part = 10000000,
                        min_rows_for_compact_part = 10000,
                        write_final_mark = 0,
                        optimize_on_insert = 1,
                        max_partitions_per_insert_block = 1000"""

    return db_config
