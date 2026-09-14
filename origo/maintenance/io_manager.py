"""The pinned filesystem IO manager with compact storage for small serialized values."""

from pathlib import Path

from dagster import (
    InitResourceContext,
    InputContext,
    MetadataValue,
    OutputContext,
    PathMetadataValue,
    io_manager,
)
from dagster._core.storage.fs_io_manager import PickledObjectFilesystemIOManager
from dagster._core.storage.upath_io_manager import UPathIOManager
from upath import UPath

from .outputs import OutputStore


class PackedFilesystemIOManager(PickledObjectFilesystemIOManager):
    def __init__(self, base_dir: str) -> None:
        self.store = OutputStore(Path(base_dir))
        self.base_dir = str(self.store.root)
        UPathIOManager.__init__(self, base_path=UPath(self.base_dir))

    def dump_to_path(self, context: OutputContext, obj: object, path: UPath) -> None:
        local = Path(str(path))
        key = self.store.key(local)
        with self.store.lock(key):
            super().dump_to_path(context, obj, path)
            if context.has_asset_key:
                self.store.pack(local)

    def load_from_path(self, context: InputContext, path: UPath) -> object:
        local = Path(str(path))
        key = self.store.key(local)
        with self.store.lock(key):
            payload = self.store.read(key)
            if payload is None:
                return super().load_from_path(context, path)
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(payload)
            try:
                return super().load_from_path(context, path)
            finally:
                local.unlink()

    def get_metadata(self, context: OutputContext, obj: object) -> dict[str, MetadataValue]:
        if not context.has_asset_key:
            return {}
        path = (
            next(iter(self._get_paths_for_partitions(context).values()))
            if context.has_asset_partitions
            else self._get_path(context)
        )
        key = self.store.key(Path(str(path)))
        if self.store.read(key) is None:
            return {}
        return {
            'path': PathMetadataValue(str(self.store.path)),
            'storage_key': MetadataValue.text(key),
        }

    def path_exists(self, path: UPath) -> bool:
        local = Path(str(path))
        key = self.store.key(local)
        with self.store.lock(key):
            return self.store.read(key) is not None or super().path_exists(path)

    def unlink(self, path: UPath) -> None:
        local = Path(str(path))
        key = self.store.key(local)
        with self.store.lock(key):
            self.store.remove(key)
            if local.exists():
                super().unlink(path)

    def _handle_transition_to_partitioned_asset(self, context: OutputContext, path: UPath) -> None:
        key = self.store.key(Path(str(path)))
        with self.store.lock(key):
            if self.store.read(key) is not None:
                self.store.remove(key)
            super()._handle_transition_to_partitioned_asset(context, path)


@io_manager
def packed_io_manager(context: InitResourceContext) -> PackedFilesystemIOManager:
    if context.instance is None:
        raise RuntimeError('Packed filesystem IO requires a persistent Dagster instance.')
    return PackedFilesystemIOManager(context.instance.storage_directory())
