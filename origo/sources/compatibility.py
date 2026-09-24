"""Refuse to deploy an image that cannot read the components production already activated."""

from typing import cast

from origo.assets.create_origo_database import (
    get_clickhouse_settings,
    make_clickhouse_client,
    table_exists,
)

from .registry import SOURCE_REGISTRY


def undeclared_components() -> dict[str, list[str]]:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    try:
        if not table_exists(client, settings, 'source_active_partitions'):
            return {}
        rows = client.execute(
            'SELECT source_key, groupUniqArrayArray(arrayMap(pair -> pair.1, '
            "JSONExtract(component_hashes, 'Array(Tuple(String, String))'))) "
            f'FROM {settings.database}.source_active_partitions GROUP BY source_key'
        )
    finally:
        client.disconnect()
    declared = {spec.key: {item.key for item in spec.components} for spec in SOURCE_REGISTRY}
    # A source the image does not register at all would silently stop being maintained.
    undeclared = {
        str(source): sorted(set(cast(list[str], keys)) - declared.get(str(source), set[str]()))
        for source, keys in rows
    }
    return {source: keys for source, keys in undeclared.items() if keys}


def main() -> None:
    undeclared = undeclared_components()
    if undeclared:
        raise SystemExit(
            f'This image cannot read components production already activated: {undeclared}. '
            'Deploy a release that declares them; rolling back past them is unsupported.'
        )
    print('Every activated component is declared by this image.', flush=True)


if __name__ == '__main__':
    main()
