import functools, hashlib, json, time, tracemalloc
from pathlib import Path
from origo.assets.create_origo_database import get_clickhouse_settings, make_clickhouse_client
from origo.query import market_state
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.storage import SourceStore
module_hash = hashlib.sha256(Path(market_state.__file__).read_bytes()).hexdigest()
assert module_hash == '0db5b29259c640decaf463bf4e201c5dc423e9706797598ee3ddae469151cc4f'
config = get_clickhouse_settings()
client = make_clickhouse_client(config)
store = SourceStore(client, config.database, BINANCE_SPOT_TRADES_SPEC)
original_record = market_state._record
cached_record = functools.lru_cache(maxsize=8192)(original_record)
execute = store.execute
observed = {}
def traced(*args, **kwargs):
    started = time.perf_counter()
    rows = execute(*args, **kwargs)
    observed.update(manifest_ms=(time.perf_counter()-started)*1000, rows=len(rows), rows_digest=hashlib.sha256(repr(rows).encode()).hexdigest())
    return rows
store.execute = traced
settings = {**market_state.QUERY_SETTINGS, 'readonly': 1, 'max_execution_time': 5, 'max_memory_usage': 536870912, 'use_query_cache': 0}
expected = digest = None
def read(kind, pair):
    global expected, digest
    market_state._record = original_record if kind == 'baseline' else cached_record
    started = time.perf_counter()
    answer = market_state.pin(store, settings)
    elapsed = (time.perf_counter()-started)*1000
    if expected is None:
        expected, digest = answer, observed['rows_digest']
    if answer != expected or observed['rows_digest'] != digest:
        raise RuntimeError('Live source state changed or decoded Pin values differ')
    print(json.dumps({'diagnostic_only': True, 'kind': kind, 'pair': pair, 'pin_ms': elapsed, 'cache': cached_record.cache_info()._asdict(), **observed}), flush=True)
print(json.dumps({'diagnostic_only': True, 'deployed_module_sha256': module_hash, 'proposal': 'Bounded memoization of the pure full-row decoder; every pin still reads its manifest; own process only', 'cache_max_entries': 8192}), flush=True)
try:
    read('baseline', 'initial')
    tracemalloc.start()
    read('candidate', 'cold')
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(json.dumps({'diagnostic_only': True, 'phase': 'allocation-observation', 'traced_current_bytes_after_cold': current, 'traced_peak_bytes': peak, 'note': 'Includes decoder cache and live client allocations; not a maximum-bound proof; cold timing includes tracing'}), flush=True)
    for pair in range(6):
        for kind in (['baseline', 'candidate'] if pair % 2 == 0 else ['candidate', 'baseline']):
            read(kind, pair)
finally:
    market_state._record = original_record
    client.disconnect()
