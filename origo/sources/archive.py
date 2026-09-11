from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from hashlib import sha256

from .contracts import SourceError

_archive: ContextVar[dict[tuple[str, str], bytes] | None] = ContextVar(
    'source_archive', default=None
)


@contextmanager
def archive_session() -> Iterator[None]:
    """Share checksum-verified bytes only within one source execution."""
    token = _archive.set({})
    try:
        yield
    finally:
        _archive.reset(token)


def verified_archive(url: str, revision: str, fetch: Callable[[str], bytes], *, code: str) -> bytes:
    cache = _archive.get()
    key = (url, revision)
    body = cache[key] if cache is not None and key in cache else fetch(url)
    if sha256(body).hexdigest() != revision:
        raise SourceError(code, 'Official archive checksum mismatch.')
    if cache is not None:
        cache.clear()
        cache[key] = body
    return body
