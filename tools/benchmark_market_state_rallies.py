"""Frozen, read-only deployment acceptance for the canonical rally transport.

Run after the merge is deployed. All observations are acquired by this command;
verify-report derives the verdict again from the immutable evidence directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Final, Protocol, cast
from uuid import UUID

import numpy as np
from numpy.typing import NDArray

from origo.sources.capacity import CAPACITY_TOTAL_RESERVE_TENTHS, CAPACITY_WORKING_SET_FACTOR
from origo.sources.contracts import RolloutStage
from origo.sources.registry import SOURCE_REGISTRY
from origo.workers.depth import DEPTH_SPECS

HOST: Final = 'root@37.27.112.167'
SCHEMA_VERSION: Final = 1
METADATA_KEY: Final = b'origo.market_state_rallies'
T0_US: Final = 1_609_459_200_000_000
BASE_US: Final = 56_250_000
BAR_US: Final = 900_000_000
ROW_BYTES: Final = 33
ABS_TOLERANCE: Final = 1e-8
REL_TOLERANCE: Final = 1e-12
SOURCE_FILES: Final = (
    'origo/query/rally_detection.py',
    'origo/query/market_state_rallies.py',
    'origo/query/market_state_reader.py',
    'origo/query/market_state_results.py',
    'origo/workers/market_state_api.py',
    'origo/sources/capacity.py',
    'origo/sources/contracts.py',
    'origo/sources/registry.py',
    'origo/workers/depth.py',
    'tools/benchmark_market_state_rallies.py',
)
FEEDS: Final = (
    ('provisional', 'binance_spot_trades', 'binance_spot_trades:mount'),
    ('provisional', 'binance_spot_aggtrades', 'binance_spot_aggtrades:mount'),
    ('provisional', 'binance_perp_aggtrades', 'binance_perp_aggtrades:mount'),
    ('provisional', 'binance_perp_trades', 'binance_perp_trades:mount'),
    ('depth', 'depth20_snapshots', None),
    ('depth', 'depth200_snapshots', None),
)
LIMITS: Final = {
    'server_seconds': 295,
    'reader_seconds': 300,
    'input_rows': 8_000_000,
    'input_bytes': 256 * 1024**2,
    'output_bytes': 512 * 1024**2,
    'worker_rss_bytes': 1536 * 1024**2,
    'container_bytes': 2 * 1024**3,
    'container_nanocpus': 2_000_000_000,
    'statement_bytes': 4 * 1024**3,
    'statement_seconds': 60,
    'statement_threads': 4,
    'store_bytes': 64 * 1024**3,
    'disk_margin_bytes': 8 * 1024**3,
    'access_p95_seconds': 1,
    'ingestion_seconds': 1800,
    'local_actions': 200,
}


class _Column(Protocol):
    def to_numpy(self, *, zero_copy_only: bool) -> NDArray[np.generic]: ...


class _Batch(Protocol):
    @property
    def num_rows(self) -> int: ...
    def column(self, name: str) -> _Column: ...
    def to_pylist(self) -> list[dict[str, object]]: ...


class _Schema(Protocol):
    @property
    def metadata(self) -> Mapping[bytes, bytes] | None: ...


class _Stream(Protocol):
    def __iter__(self) -> Iterator[_Batch]: ...


class _File(Protocol):
    @property
    def schema(self) -> _Schema: ...
    @property
    def num_record_batches(self) -> int: ...
    def get_batch(self, index: int) -> _Batch: ...


class _IPC(Protocol):
    def open_stream(self, source: object) -> _Stream: ...
    def open_file(self, source: object) -> _File: ...


ipc = cast(_IPC, import_module('pyarrow.ipc'))


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f'Expected object; received {type(value).__name__}.')
    return dict(cast(Mapping[str, object], value))


def _list(value: object) -> list[object]:
    if not isinstance(value, (list, tuple)):
        raise ValueError(f'Expected array; received {type(value).__name__}.')
    return list(cast(Sequence[object], value))


def _integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError('Expected integer.')
    return value


def _float(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('Expected finite number.')
    return float(value)


def _utc(value: object) -> datetime:
    at = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if at.utcoffset() is None:
        raise ValueError('Expected timezone-aware UTC timestamp.')
    return at.astimezone(UTC)


def _stamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def _us(value: datetime) -> int:
    delta = value - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def _at(value: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=value)


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024**2), b''):
            digest.update(block)
    return digest.hexdigest()


def _rank(values: Sequence[float], quantile: float) -> float:
    if not values:
        raise ValueError('No measured samples.')
    return sorted(values)[max(0, math.ceil(len(values) * quantile) - 1)]


def _definition(mode: str, scale: str, *, small: bool = False) -> dict[str, object]:
    target = (0.1 if small else 1) if scale == 'atr' else (1 if small else 30)
    result: dict[str, object] = {'mode': mode, 'scale': scale, 'target': target}
    if mode != 'swing':
        result['anchor_minutes'] = 1
    if mode == 'controlled_advance':
        result['pullback'] = (1 if small else 0.5) if scale == 'atr' else (100 if small else 10)
    if mode == 'swing':
        result['reversal'] = (0.1 if small else 0.5) if scale == 'atr' else (1 if small else 10)
    return result


def _required_heartbeats() -> list[str]:
    names = {
        f'provisional_{spec.key}.heartbeat'
        for spec in SOURCE_REGISTRY
        if spec.provisional is not None and spec.rollout_stage != RolloutStage.DORMANT
    } | {'market_state_api.heartbeat', 'perp_capture.heartbeat'}
    if DEPTH_SPECS:
        names.add('depth.heartbeat')
    return sorted(names)


def frozen_manifest() -> dict[str, object]:
    windows = (
        ('minute16', '2026-06-27T11:39:00Z', '2026-06-27T11:55:00Z', ('bps', 'atr')),
        ('hour1', '2026-06-27T10:00:00Z', '2026-06-27T11:00:00Z', ('bps', 'atr')),
        ('hour24', '2026-06-26T00:00:00Z', '2026-06-27T00:00:00Z', ('bps', 'atr')),
        ('long_bps', '2026-06-25T00:00:00Z', '2026-06-27T00:00:00Z', ('bps',)),
        ('long_atr', '2026-06-25T03:45:00Z', '2026-06-27T00:00:00Z', ('atr',)),
    )
    cases: list[dict[str, object]] = []
    for name, start, end, scales in windows:
        for scale in scales:
            for mode in ('first_hit', 'controlled_advance', 'swing'):
                origin = _us(_utc(start))
                bulk_start = origin if scale == 'bps' else origin // BAR_US * BAR_US - 15 * BAR_US
                cases.append(
                    {
                        'id': f'{name}_{scale}_{mode}',
                        'definition': _definition(mode, scale),
                        'analysis': {'start': start, 'end': end},
                        'bulk': {'start': _stamp(_at(bulk_start)), 'end': _stamp(_utc(end))},
                        'reference_max_seconds': 0 if mode == 'swing' else 86400,
                    }
                )
    proof_cases: list[dict[str, object]] = [
        {
            'id': f'positive_{scale}_{mode}',
            'definition': _definition(mode, scale, small=True),
            'analysis': {'start': '2026-06-27T11:39:00Z', 'end': '2026-06-27T11:55:00Z'},
        }
        for scale in ('bps', 'atr')
        for mode in ('controlled_advance', 'swing')
    ]
    for name, mode, start, end in (
        ('empty', 'first_hit', '2026-06-27T11:39:00.000001Z', '2026-06-27T11:39:00.000002Z'),
        ('left', 'swing', '2026-06-27T11:39:00Z', '2026-06-27T11:39:00.000001Z'),
        ('right', 'first_hit', '2026-06-27T11:39:00Z', '2026-06-27T11:39:00.000001Z'),
        ('unknown', 'first_hit', '2021-01-01T00:00:00Z', '2021-01-01T00:01:00Z'),
    ):
        proof_cases.append(
            {
                'id': name,
                'definition': _definition(mode, 'bps'),
                'analysis': {'start': start, 'end': end},
            }
        )
    for case in (*cases, *proof_cases):
        definition = _object(case['definition'])
        analysis = _object(case['analysis'])
        origin, end = _us(_utc(analysis['start'])), _us(_utc(analysis['end']))
        anchor = -(-origin // 60_000_000) * 60_000_000
        probe_windows: list[list[str]] = []
        if (
            definition['mode'] != 'swing'
            and anchor < end
            and max(T0_US, anchor - 86400_000_000) < anchor
        ):
            probe_windows = [[_stamp(_at(max(T0_US, anchor - 86400_000_000))), _stamp(_at(anchor))]]
        case['reference_windows'] = probe_windows
        case['normalized_definition'] = {
            key: (str(value) if key in ('target', 'pullback', 'reversal') else value)
            for key, value in definition.items()
        }
        case['definition_fingerprint'] = _fingerprint(definition)
    return {
        'schema_version': SCHEMA_VERSION,
        'source': 'binance_spot_trades',
        'instrument': 'BTCUSDT',
        'production_host': HOST,
        'cases': cases,
        'proof_cases': proof_cases,
        'rounds': ['cold', 'warm1', 'warm2', 'warm3', 'warm4', 'warm5'],
        'cold_definition': 'First discovery of each definition/origin in this run; no production cache eviction.',
        'limits': dict(LIMITS),
        'required_heartbeats': _required_heartbeats(),
        'tolerance': {
            'absolute_usdt': ABS_TOLERANCE,
            'relative': REL_TOLERANCE,
            'rationale': 'Float64 literal thresholds; math.fsum native quote oracle, absolute rounding near zero and relative accumulation scale; exact integer counts.',
        },
        'corrected_revision': {
            'status': 'deferred',
            'reason': 'No authentic production before-and-after corrected revision is available; no generated correction is admissible.',
        },
    }


# Read-only host and container RPCs are embedded so acceptance needs no installed SSH helper.
# They never print environment variables or credentials and never reset production caches/counters.
_CONTAINER_SCRIPT = r"""
import hashlib,json,os,sys
from pathlib import Path
from importlib import import_module
from datetime import datetime,timezone
from origo.assets.create_origo_database import get_clickhouse_settings,make_clickhouse_client
from origo.query.market_state import pin,iso
from origo.sources.storage import SourceStore
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
p=json.load(sys.stdin)
s={'max_threads':1,'max_memory_usage':4294967296,'max_execution_time':60,'max_bytes_before_external_group_by':0,'max_bytes_before_external_sort':0,'max_bytes_ratio_before_external_group_by':0,'max_bytes_ratio_before_external_sort':0,'log_comment':'rally_acceptance_readonly'}
cfg=get_clickhouse_settings();c=make_clickhouse_client(cfg)
def clean(v):
    if isinstance(v,datetime): return (v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)).isoformat(timespec='microseconds')
    if isinstance(v,(tuple,list)): return [clean(x) for x in v]
    if isinstance(v,dict): return {str(k):clean(x) for k,x in v.items()}
    if isinstance(v,(str,int,float,bool)) or v is None: return v
    return str(v)
try:
    if p['action']=='pin':
        state=pin(SourceStore(c,cfg.database,BINANCE_SPOT_TRADES_SPEC),s)
        records=[{'key':r.partition.key,'start':iso(r.partition.start),'end':iso(r.partition.end),'provisional':r.partition.provisional,'revision':r.revision,'build_id':str(r.build_id),'generation':r.generation} for r in state.records]
        cutoff=iso(state.cutoff);pairs=sorted((r['key'],[r['revision'],r['build_id']]) for r in records)
        out={'data_cutoff':cutoff,'canonical_through':iso(state.canonical_through),'records':records,'pack_pin_digest':hashlib.sha256(json.dumps([cutoff,pairs],separators=(',',':')).encode()).hexdigest()}
        print(json.dumps(out))
    elif p['action']=='query':
        q=p['sql']
        if not q.lstrip().upper().startswith('SELECT ') or ';' in q: raise ValueError('Only one SELECT is permitted.')
        rows,cols=c.execute(q,p.get('params',{}),settings=s,with_column_types=True)
        print(json.dumps({'columns':cols,'rows':clean(rows),'captured_at':clean(datetime.now(timezone.utc)),'sql':q,'params':p.get('params',{}),'settings':s}))
    elif p['action']=='native':
        import clickhouse_connect
        client=clickhouse_connect.get_client(host=cfg.host,port=int(os.environ.get('CLICKHOUSE_HTTP_PORT','8123')),username=cfg.user,password=cfg.password,autogenerate_session_id=False,send_receive_timeout=75)
        q=p['sql']
        if not q.lstrip().upper().startswith('SELECT ') or ';' in q: raise ValueError('Only one SELECT is permitted.')
        try:
            with client.raw_stream(q,settings=s,fmt='ArrowStream') as stream:
                for block in stream: sys.stdout.buffer.write(block)
        finally: client.close()
    else: raise ValueError('Unknown container RPC action.')
finally: c.disconnect()
"""

_HOST_SCRIPT = r"""
import hashlib,json,os,re,sqlite3,subprocess,sys,urllib.request,urllib.error
from pathlib import Path
from importlib import import_module
from datetime import datetime,timezone
p=json.load(sys.stdin)
def run(args): return subprocess.check_output(args)
def inspect(service):
    ids=run(['docker','ps','--filter','label=com.docker.compose.service='+service,'--format','{{.ID}}']).decode().splitlines()
    if len(ids)!=1: raise RuntimeError('Expected one running '+service+' container; got '+str(len(ids)))
    raw=json.loads(run(['docker','inspect',ids[0]]))[0]
    return {'id':ids[0],'image':raw['Config']['Image'],'pid':raw['State']['Pid'],'memory':raw['HostConfig']['Memory'],'nanocpus':raw['HostConfig']['NanoCpus'],'oom_killed':raw['State']['OOMKilled'],'mounts':[{'source':x['Source'],'destination':x['Destination']} for x in raw['Mounts']]}
def post(route,body,timeout=300):
    if route not in ('rallies','query','access'): raise ValueError('Unrecognized service route.')
    request=urllib.request.Request(p['url']+'/v1/market-state/'+route,data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(request,timeout=timeout) as response: return {'status':response.status,'body':json.load(response)}
    except urllib.error.HTTPError as e: return {'status':e.code,'body':json.loads(e.read())}
def mount(info,dest):
    matches=[Path(x['source']) for x in info['mounts'] if x['destination']==dest]
    if len(matches)!=1: raise RuntimeError('Missing unique mount '+dest)
    return matches[0]
def snapshot():
    info=inspect('market-state');root=mount(info,'/opt/origo/market-state');stat=os.statvfs(root)
    status=Path('/proc/'+str(info['pid'])+'/status').read_text();rss={x.split(':')[0]:int(x.split()[1])*1024 for x in status.splitlines() if x.startswith(('VmRSS:','VmHWM:'))}
    cg=Path('/sys/fs/cgroup'+Path('/proc/'+str(info['pid'])+'/cgroup').read_text().strip().split(':',2)[2]);mem={n:(cg/n).read_text().strip() for n in ('memory.current','memory.peak','memory.max','memory.events','cpu.max')}
    db=sqlite3.connect('file:'+str(root/'lifecycle.sqlite')+'?mode=ro',uri=True)
    try:
        results=list(db.execute('SELECT result_id,state,created_ns FROM results ORDER BY result_id'))
        files=list(db.execute('SELECT result_id,name,bytes,last_access_ns,retired FROM files ORDER BY result_id,name'))
    finally: db.close()
    result_bytes=sum(x.stat().st_size for x in (root/'results').rglob('*') if x.is_file());staging_bytes=sum(x.stat().st_size for x in (root/'staging').rglob('*') if x.is_file())
    stored=sum(x[2] for x in files);sizes={};
    for x in files: sizes[x[0]]=sizes.get(x[0],0)+x[2]
    staged_sizes=[sum(f.stat().st_size for f in (root/'staging'/x[0]).glob('*') if f.is_file()) for x in results if x[1]=='staging'];reserved=sum(max(536870912,size) for size in staged_sizes);pending=sum(max(536870912-size,0) for size in staged_sizes)
    heartbeats=[]
    for h in info['mounts']:
        if h['destination']=='/opt/origo/heartbeats':
            heartbeats=[{'name':x.name,'mtime':x.stat().st_mtime} for x in Path(h['source']).glob('*.heartbeat') if x.name != 'provisional.heartbeat']
    return {'at':datetime.now(timezone.utc).isoformat(timespec='microseconds'),'container':info,'rss':rss,'cgroup':mem,'results':results,'files':files,'stored_bytes':stored,'reserved_bytes':reserved,'pending_bytes':pending,'staging_count':len(staged_sizes),'total_bytes':stat.f_blocks*stat.f_frsize,'total_inodes':stat.f_files,'result_bytes':result_bytes,'staging_bytes':staging_bytes,'free_bytes':stat.f_bavail*stat.f_frsize,'free_inodes':stat.f_favail,'heartbeats':heartbeats}
a=p['action']
if a=='dagit':
    request=urllib.request.Request('http://127.0.0.1:4000/graphql',data=json.dumps({'query':p['query']}).encode(),headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(request,timeout=30) as response: print(json.dumps(json.load(response)))
elif a=='preflight':
    info=inspect('market-state');digests={}
    for name in p['files']:
        if not re.fullmatch(r'(origo/[a-z_/]+|tools/benchmark_market_state_rallies)\.py',name): raise ValueError('Invalid source file.')
        digests[name]=hashlib.sha256(run(['docker','exec',info['id'],'cat','/opt/app/'+name])).hexdigest()
    print(json.dumps({'deployment':info,'source_files':digests,'snapshot':snapshot()}))
elif a=='snapshot': print(json.dumps(snapshot()))
elif a=='post': print(json.dumps(post(p['route'],p['body'],p.get('timeout',300))))
elif a=='file':
    info=inspect('market-state');root=mount(info,'/opt/origo/market-state');rid=p['result_id'];name=p['name']
    if not re.fullmatch(r'[0-9a-f-]{36}',rid) or name not in ('rallies.arrow','rally_cells.arrow','summary.arrow'): raise ValueError('Invalid result path.')
    path='/opt/origo/market-state/results/'+rid+'/'+name
    renewed=post('access',{'path':path},10)
    if renewed['status']!=200: raise RuntimeError('Renewal refused '+str(renewed))
    with (root/'results'/rid/name).open('rb') as handle:
        for block in iter(lambda:handle.read(1048576),b''): sys.stdout.buffer.write(block)
elif a in ('pin','query','native'):
    info=inspect('dagster');subprocess.run(['docker','exec','-i',info['id'],'python','-c',p['container_script']],input=json.dumps(p).encode(),check=True)
elif a=='reader':
    info=inspect('market-state')
    code="import json,sys,time;from origo.query.market_state_reader import open_file;p=json.load(sys.stdin);t=time.perf_counter();r=open_file(p['path'],url='http://127.0.0.1:8486');n=sum(r.get_batch(i).num_rows for i in range(r.num_record_batches));print(json.dumps({'seconds':time.perf_counter()-t,'rows':n,'metadata':r.schema.metadata[b'origo.market_state_rallies'].decode()}))"
    subprocess.run(['docker','exec','-i',info['id'],'python','-c',code],input=json.dumps(p).encode(),check=True)
else: raise ValueError('Unknown host RPC action.')
"""


class Production:
    def __init__(self, url: str) -> None:
        if not re.fullmatch(r'http://127\.0\.0\.1:\d{1,5}', url):
            raise ValueError('Use the deployed service loopback URL, http://127.0.0.1:<port>.')
        self.url = url

    def _payload(self, action: str, fields: Mapping[str, object]) -> bytes:
        return _json(
            {'action': action, 'url': self.url, 'container_script': _CONTAINER_SCRIPT, **fields}
        )

    @staticmethod
    def _command() -> list[str]:
        return [
            'ssh',
            '-o',
            'BatchMode=yes',
            '-o',
            'ConnectTimeout=10',
            HOST,
            'python3 -c ' + shlex.quote(_HOST_SCRIPT),
        ]

    def read(self, action: str, **fields: object) -> dict[str, object]:
        result = subprocess.run(
            self._command(),
            input=self._payload(action, fields),
            capture_output=True,
            check=True,
            timeout=330,
        )
        return _object(json.loads(result.stdout))

    def stream(self, path: Path, action: str, **fields: object) -> None:
        with path.open('xb') as handle:
            subprocess.run(
                self._command(),
                input=self._payload(action, fields),
                stdout=handle,
                check=True,
                timeout=330,
            )
            handle.flush()
            os.fsync(handle.fileno())

    def query(self, sql: str, params: Mapping[str, object] | None = None) -> dict[str, object]:
        return self.read('query', sql=sql, params=dict(params or {}))


class Archive:
    def __init__(self, report: Path) -> None:
        self.report = report.resolve()
        self.root = self.report.with_suffix('.evidence')
        self.root.mkdir(parents=True, exist_ok=False)
        self.entries: list[dict[str, object]] = []

    def record(self, name: str, value: object) -> str:
        path = self.root / name
        with path.open('xb') as handle:
            handle.write(_json(value) + b'\n')
            handle.flush()
            os.fsync(handle.fileno())
        self.register(name)
        return name

    def register(self, name: str) -> None:
        path = self.root / name
        self.entries.append({'file': name, 'sha256': _sha(path), 'bytes': path.stat().st_size})

    def load(self, name: str) -> dict[str, object]:
        return _object(json.loads((self.root / name).read_bytes()))

    def finish(self, report: dict[str, object]) -> None:
        report['evidence'] = self.entries
        with self.report.open('xb') as handle:
            handle.write(_json(report) + b'\n')
            handle.flush()
            os.fsync(handle.fileno())
        checksum = self.report.with_suffix(self.report.suffix + '.sha256')
        with checksum.open('x') as handle:
            handle.write(_sha(self.report) + '\n')
        for path in (*self.root.iterdir(), self.report, checksum):
            path.chmod(0o444)
        self.root.chmod(0o555)


@dataclass(frozen=True)
class Native:
    ids: NDArray[np.uint64]
    times: NDArray[np.int64]
    prices: NDArray[np.float64]
    quotes: NDArray[np.float64]
    makers: NDArray[np.bool_]

    def bounds(self, low: int, high: int) -> tuple[int, int]:
        return int(np.searchsorted(self.times, low)), int(np.searchsorted(self.times, high))


def _native(path: Path, prefix: Native | None = None) -> Native:
    with path.open('rb') as handle:
        count = 0
        first_at: int | None = None
        for batch in ipc.open_stream(handle):
            count += batch.num_rows
            if first_at is None and batch.num_rows:
                first_at = int(batch.column('timestamp_us').to_numpy(zero_copy_only=False)[0])
    prepend = (
        1
        if prefix is not None
        and len(prefix.ids)
        and (first_at is None or int(prefix.times[0]) < first_at)
        else 0
    )
    if (
        count + prepend > LIMITS['input_rows']
        or (count + prepend) * ROW_BYTES > LIMITS['input_bytes']
    ):
        raise ValueError('Oracle native columns exceed the frozen input budget.')
    arrays: list[NDArray[np.generic]] = [
        np.empty(count + prepend, dtype)
        for dtype in (np.uint64, np.int64, np.float64, np.float64, np.bool_)
    ]
    if prepend and prefix is not None:
        for array, source in zip(
            arrays,
            (prefix.ids, prefix.times, prefix.prices, prefix.quotes, prefix.makers),
            strict=True,
        ):
            array[0] = source[0]
    names = ('trade_id', 'timestamp_us', 'price', 'quote_quantity', 'is_buyer_maker')
    offset = prepend
    with path.open('rb') as handle:
        for batch in ipc.open_stream(handle):
            for name, array in zip(names, arrays, strict=True):
                array[offset : offset + batch.num_rows] = batch.column(name).to_numpy(
                    zero_copy_only=False
                )
            offset += batch.num_rows
    return Native(
        cast(NDArray[np.uint64], arrays[0]),
        cast(NDArray[np.int64], arrays[1]),
        cast(NDArray[np.float64], arrays[2]),
        cast(NDArray[np.float64], arrays[3]),
        cast(NDArray[np.bool_], arrays[4]),
    )


def _fingerprint(definition: Mapping[str, object]) -> str:
    normalized = {
        key: (str(value) if key in ('target', 'pullback', 'reversal') else value)
        for key, value in definition.items()
    }
    return hashlib.sha256(b'rally_v1\n' + _json(normalized)).hexdigest()


def _bars(raw: Native) -> dict[int, tuple[float, float, float]]:
    periods = raw.times // BAR_US * BAR_US
    starts = (
        np.flatnonzero(np.r_[True, periods[1:] != periods[:-1]])
        if len(periods)
        else np.empty(0, np.int64)
    )
    bars: dict[int, tuple[float, float, float]] = {}
    for first, stop in zip(starts, np.r_[starts[1:], len(periods)], strict=True):
        prices = raw.prices[int(first) : int(stop)]
        bars[int(periods[first])] = (
            float(np.max(prices)),
            float(np.min(prices)),
            float(prices[-1]),
        )
    return bars


def _atr(bars: Mapping[int, tuple[float, float, float]], at: int) -> float:
    end = at // BAR_US * BAR_US
    required = [bars[start] for start in range(end - 15 * BAR_US, end, BAR_US)]
    ranges = [
        max(high - low, abs(high - required[index - 1][2]), abs(low - required[index - 1][2]))
        for index, (high, low, _) in enumerate(required)
        if index
    ]
    return math.fsum(ranges) / 14


def _extents(
    raw: Native, case: Mapping[str, object]
) -> Iterator[tuple[int | None, int, int, int, int]]:
    """Independent nested leg scans; no production detector or detector helper is called."""
    definition = _object(case['definition'])
    analysis = _object(case['analysis'])
    origin, edge = _us(_utc(analysis['start'])), _us(_utc(analysis['end']))
    first, stop = raw.bounds(origin, edge)
    atr_mode = definition['scale'] == 'atr'
    target = _float(definition['target'])
    bars = _bars(raw) if atr_mode else {}
    if definition['mode'] != 'swing':
        cadence = _integer(definition['anchor_minutes']) * 60_000_000
        for anchor in range(-(-origin // cadence) * cadence, edge, cadence):
            begin = int(np.searchsorted(raw.times, anchor))
            reference = begin - 1
            if reference < 0 or int(raw.times[reference]) < anchor - 86400_000_000:
                continue
            price = float(raw.prices[reference])
            frozen = _atr(bars, anchor) if atr_mode else price / 10000
            threshold = price + target * frozen if atr_mode else price * (1 + target / 10000)
            finish = int(np.searchsorted(raw.times, min(edge, anchor + 240 * 60_000_000)))
            hits = np.flatnonzero(raw.prices[begin:finish] >= threshold)
            if len(hits):
                hit = begin + int(hits[0])
                if definition['mode'] == 'controlled_advance':
                    peak = price
                    invalid = False
                    for value in raw.prices[begin : hit + 1]:
                        peak = max(peak, float(value))
                        if peak - float(value) > _float(definition['pullback']) * frozen:
                            invalid = True
                            break
                    if invalid:
                        continue
                yield anchor, reference, begin, hit, hit
    elif first < stop:
        reversal = _float(definition['reversal'])
        cursor, extreme = first + 1, first
        while cursor < stop:
            if raw.prices[cursor] > raw.prices[extreme]:
                extreme = cursor
            frozen = (
                _atr(bars, int(raw.times[extreme]))
                if atr_mode
                else float(raw.prices[extreme]) / 10000
            )
            barrier = (
                float(raw.prices[extreme]) - reversal * frozen
                if atr_mode
                else float(raw.prices[extreme]) * (1 - reversal / 10000)
            )
            if float(raw.prices[cursor]) <= barrier:
                break
            cursor += 1
        while cursor < stop:
            trough = cursor
            frozen = (
                _atr(bars, int(raw.times[trough]))
                if atr_mode
                else float(raw.prices[trough]) / 10000
            )
            cursor += 1
            while cursor < stop:
                if raw.prices[cursor] < raw.prices[trough]:
                    trough = cursor
                frozen = (
                    _atr(bars, int(raw.times[trough]))
                    if atr_mode
                    else float(raw.prices[trough]) / 10000
                )
                threshold = (
                    float(raw.prices[trough]) + target * frozen
                    if atr_mode
                    else float(raw.prices[trough]) * (1 + target / 10000)
                )
                if float(raw.prices[cursor]) >= threshold:
                    break
                cursor += 1
            if cursor == stop:
                break
            peak = cursor
            cursor += 1
            while cursor < stop:
                if raw.prices[cursor] > raw.prices[peak]:
                    peak = cursor
                barrier = (
                    float(raw.prices[peak]) - reversal * frozen
                    if atr_mode
                    else float(raw.prices[peak]) * (1 - reversal / 10000)
                )
                if float(raw.prices[cursor]) <= barrier:
                    yield None, trough, trough, peak, cursor
                    break
                cursor += 1


def _member_rows(raw: Native, first: int, stop: int) -> Iterator[dict[str, object]]:
    keys: dict[tuple[int, int], list[int]] = defaultdict(list)
    for index in range(first, stop):
        key = (
            (int(raw.times[index]) - T0_US) // BASE_US,
            math.floor(float(raw.prices[index]) / 125),
        )
        keys[key].append(index)
    for (time_index, price_index), positions in sorted(keys.items()):
        yield {
            'base_time_index': time_index,
            'base_price_index': price_index,
            'volume': math.fsum(float(raw.quotes[i]) for i in positions),
            'trade_count': len(positions),
            'taker_buy_volume': math.fsum(
                float(raw.quotes[i]) for i in positions if not raw.makers[i]
            ),
            'taker_buy_trade_count': sum(not bool(raw.makers[i]) for i in positions),
            'first_trade_id': int(raw.ids[positions[0]]),
            'last_trade_id': int(raw.ids[positions[-1]]),
            'first_at': _at(int(raw.times[positions[0]])),
            'last_at': _at(int(raw.times[positions[-1]])),
        }


def _event(
    raw: Native, case: Mapping[str, object], extent: tuple[int | None, int, int, int, int]
) -> dict[str, object]:
    anchor, reference, first, last, confirmation = extent
    definition = _object(case['definition'])
    fingerprint = _fingerprint(definition)
    prefix = f'binance_spot_trades:BTCUSDT:rally_v1:{fingerprint}'
    rally_id = (
        f'{prefix}:o{_us(_utc(_object(case["analysis"])["start"]))}:t{int(raw.ids[reference])}'
        if anchor is None
        else f'{prefix}:a{anchor}'
    )
    if definition == {'mode': 'first_hit', 'scale': 'bps', 'target': 30, 'anchor_minutes': 1}:
        rally_id = f'binance:spot:BTCUSDT:r30v1:t{cast(int, anchor) // 1_000_000}'
    peak = float(raw.prices[reference]) if anchor is not None else float(raw.prices[first])
    drawdown = 0.0
    for price in raw.prices[first : last + 1]:
        peak = max(peak, float(price))
        drawdown = max(drawdown, peak - float(price))
    return {
        'rally_id': rally_id,
        'definition_version': 'rally_v1',
        'definition_fingerprint': fingerprint,
        'anchor_at': None if anchor is None else _at(anchor),
        'reference_trade_id': int(raw.ids[reference]),
        'reference_at': _at(int(raw.times[reference])),
        'reference_price': float(raw.prices[reference]),
        'start_trade_id': int(raw.ids[first]),
        'start_at': _at(int(raw.times[first])),
        'start_price': float(raw.prices[first]),
        'end_trade_id': int(raw.ids[last]),
        'end_at': _at(int(raw.times[last])),
        'end_price': float(raw.prices[last]),
        'confirmation_trade_id': int(raw.ids[confirmation]),
        'confirmed_at': _at(int(raw.times[confirmation])),
        'return_bps': (float(raw.prices[last]) / float(raw.prices[reference]) - 1) * 10000,
        'duration_seconds': (
            int(raw.times[last]) - (int(raw.times[reference]) if anchor is None else anchor)
        )
        / 1_000_000,
        'max_drawdown': drawdown,
        'volume': math.fsum(float(x) for x in raw.quotes[first : last + 1]),
        'trade_count': last - first + 1,
        'taker_buy_volume': math.fsum(
            float(raw.quotes[i]) for i in range(first, last + 1) if not raw.makers[i]
        ),
        'taker_buy_trade_count': sum(not bool(x) for x in raw.makers[first : last + 1]),
    }


def _hash_row(row: Mapping[str, object]) -> bytes:
    record: dict[str, object] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            record[key] = _stamp(value)
        elif isinstance(value, float):
            record[key] = value.hex()
        elif key.endswith('trade_id') or key in (
            'trade_count',
            'taker_buy_trade_count',
            'base_time_index',
            'base_price_index',
        ):
            record[key] = str(value)
        else:
            record[key] = value
    return _json(record)


def _rows(path: Path) -> Iterator[dict[str, object]]:
    reader = ipc.open_file(str(path))
    for index in range(reader.num_record_batches):
        yield from reader.get_batch(index).to_pylist()


def _metadata(path: Path) -> dict[str, object]:
    reader = ipc.open_file(str(path))
    metadata = reader.schema.metadata
    if metadata is None or METADATA_KEY not in metadata:
        raise ValueError('Canonical file metadata is missing.')
    return _object(json.loads(metadata[METADATA_KEY]))


def _equal(observed: Mapping[str, object], expected: Mapping[str, object], label: str) -> None:
    for key, value in expected.items():
        actual = observed[key]
        if isinstance(value, float) and key in ('volume', 'taker_buy_volume', 'whole_base_volume'):
            if not math.isclose(
                _float(actual), value, abs_tol=ABS_TOLERANCE, rel_tol=REL_TOLERANCE
            ):
                raise ValueError(f'{label}: {key} differs: {actual} versus {value}.')
        elif actual != value:
            raise ValueError(f'{label}: {key} differs: {actual} versus {value}.')


def oracle(
    raw: Native, case: Mapping[str, object], events_path: Path, cells_path: Path
) -> dict[str, object]:
    expected: dict[str, tuple[dict[str, object], int, int]] = {}
    for extent in _extents(raw, case):
        event = _event(raw, case, extent)
        expected[str(event['rally_id'])] = (event, extent[2], extent[3] + 1)
    observed: dict[str, dict[str, object]] = {}
    for row in _rows(events_path):
        rally_id = str(row['rally_id'])
        if rally_id in observed:
            raise ValueError('Duplicate canonical event identity.')
        observed[rally_id] = row
    if observed.keys() != expected.keys():
        raise ValueError(
            f'{case["id"]}: exact event identities differ ({len(observed)} versus {len(expected)}).'
        )
    cells = iter(_rows(cells_path))
    current = next(cells, None)
    endpoint_pairs: dict[int, int] = defaultdict(int)
    flags = {'partial': 0, 'endpoint_overlap': 0, 'outside_endpoint_prices': 0, 'timestamp_ties': 0}
    member_cells = 0
    for rally_id in sorted(expected):
        event, first, stop = expected[rally_id]
        actual = observed[rally_id]
        _equal(actual, event, rally_id)
        digest = hashlib.sha256(
            b'rally_evidence_v1\n' + _hash_row({key: actual[key] for key in event}) + b'\nmember_trade_ids_le_u64\n'
        )
        digest.update(raw.ids[first:stop].astype('<u8', copy=False).tobytes())
        digest.update(b'\nmember_base_rows\n')
        endpoint_pairs[_integer(event['end_trade_id'])] += 1
        flags['timestamp_ties'] += int(
            np.count_nonzero(raw.times[first + 1 : stop] == raw.times[first : stop - 1])
        )
        for member in _member_rows(raw, first, stop):
            if current is None or current['rally_id'] != rally_id:
                raise ValueError('Missing canonical member cell.')
            _equal(current, member, rally_id)
            whole_first = T0_US + _integer(member['base_time_index']) * BASE_US
            low, high = raw.bounds(whole_first, whole_first + BASE_US)
            whole_indices = (
                np.flatnonzero(
                    np.floor(raw.prices[low:high] / 125) == _integer(member['base_price_index'])
                )
                + low
            )
            whole_count = len(whole_indices)
            if whole_count < _integer(member['trade_count']):
                raise ValueError('Oracle capture lacks complete whole-cell context.')
            _equal(
                current,
                {
                    'whole_base_trade_count': whole_count,
                    'whole_base_volume': math.fsum(float(raw.quotes[i]) for i in whole_indices),
                    'partial': _integer(member['trade_count']) < whole_count,
                },
                rally_id,
            )
            flags['partial'] += int(bool(current['partial']))
            lower = math.floor(min(_float(event['start_price']), _float(event['end_price'])) / 125)
            upper = math.floor(max(_float(event['start_price']), _float(event['end_price'])) / 125)
            flags['outside_endpoint_prices'] += int(
                not lower <= _integer(member['base_price_index']) <= upper
            )
            digest.update(_hash_row({key: current[key] for key in member}) + b'\n')
            member_cells += 1
            current = next(cells, None)
        if actual['evidence_version'] != 1 or actual['event_evidence_hash'] != digest.hexdigest():
            raise ValueError(f'{rally_id}: event evidence content hash differs.')
    if current is not None:
        raise ValueError('Extra canonical member cell.')
    flags['endpoint_overlap'] = sum(count > 1 for count in endpoint_pairs.values())
    return {
        'case_id': case['id'],
        'event_count': len(expected),
        'member_cells': member_cells,
        'content_hashes': {key: observed[key]['event_evidence_hash'] for key in sorted(observed)},
        'authentic_features': flags,
    }


def _records(evidence: Mapping[str, object]) -> list[dict[str, object]]:
    columns = [str(_list(column)[0]) for column in _list(evidence['columns'])]
    return [dict(zip(columns, _list(row), strict=True)) for row in _list(evidence['rows'])]


def _raw_sql(state: Mapping[str, object], low: int, high: int, *, probe: bool = False) -> str:
    statements: list[str] = []
    for provisional, table in ((False, 'raw_revisions'), (True, 'raw_latest_revisions')):
        selected: list[str] = []
        for value in _list(state['records']):
            record = _object(value)
            if (
                record['provisional'] == provisional
                and _us(_utc(record['start'])) < high
                and _us(_utc(record['end'])) > low
            ):
                key, revision = str(record['key']), str(record['revision'])
                build = str(UUID(str(record['build_id'])))
                if not re.fullmatch(r'[0-9T:.Z+-]+', key) or not re.fullmatch(
                    r'[0-9a-f]{64}', revision
                ):
                    raise ValueError('Unexpected native pin grammar.')
                selected.append(f"('{key}','{revision}',toUUID('{build}'))")
        if selected:
            statements.append(
                'SELECT trade_id,toUnixTimestamp64Micro(datetime) AS timestamp_us,price,quote_quantity,'
                'toBool(is_buyer_maker) AS is_buyer_maker '
                f'FROM origo.binance_spot_trades_{table} '
                f"WHERE source_date >= '{_at(low).date()}' AND source_date <= '{_at(high - 1).date()}' "
                f'AND (partition_key,revision,build_id) IN ({",".join(selected)}) '
                f"AND datetime >= fromUnixTimestamp64Micro({low},'UTC') "
                f"AND datetime < fromUnixTimestamp64Micro({high},'UTC')"
            )
    if not statements:
        raise ValueError('No authentic held native pin covers this oracle read.')
    union = ' UNION ALL '.join(statements)
    return f'SELECT * FROM ({union}) ORDER BY trade_id' + (' DESC LIMIT 1' if probe else '')


def _capture_native(
    prod: Production, archive: Archive, state: Mapping[str, object], case: Mapping[str, object]
) -> str:
    analysis, definition = _object(case['analysis']), _object(case['definition'])
    origin, end = _us(_utc(analysis['start'])), _us(_utc(analysis['end']))
    low = origin if definition['scale'] == 'bps' else origin // BAR_US * BAR_US - 15 * BAR_US
    # Whole base cells are independent oracle context, not part of the API bulk accounting.
    low = max(T0_US, min(low, T0_US + (origin - T0_US) // BASE_US * BASE_US))
    high = T0_US + -(-(end - T0_US) // BASE_US) * BASE_US
    sql = _raw_sql(state, low, high)
    count = prod.query(f'SELECT count() AS rows FROM ({sql})')
    rows = _integer(_records(count)[0]['rows'])
    if rows > LIMITS['input_rows'] or rows * ROW_BYTES > LIMITS['input_bytes']:
        raise ValueError('Authentic native oracle capture exceeds frozen bounded-column budget.')
    prefix = str(case['id'])
    archive.record(
        prefix + '-native-provenance.json',
        {
            'source': 'binance_spot_trades',
            'instrument': 'BTCUSDT',
            'held_state': 'held-state.json',
            'sql': sql,
            'count_evidence': count,
            'count': rows,
            'low_us': low,
            'high_us': high,
            'captured_at': _stamp(datetime.now(UTC)),
            'native_values': 'unchanged',
            'settings': {
                'max_threads': 1,
                'max_memory_usage': 4 * 1024**3,
                'max_execution_time': 60,
                'max_bytes_before_external_group_by': 0,
                'max_bytes_before_external_sort': 0,
                'max_bytes_ratio_before_external_group_by': 0,
                'max_bytes_ratio_before_external_sort': 0,
            },
        },
    )
    path = archive.root / (prefix + '-native.arrow')
    prod.stream(path, 'native', sql=sql)
    archive.register(path.name)
    raw = _native(path)
    if len(raw.ids) != rows:
        raise ValueError('Native oracle capture count changed.')
    if definition['mode'] != 'swing':
        anchor = -(-origin // 60_000_000) * 60_000_000
        if anchor < end:
            probe_low = max(T0_US, anchor - 86400_000_000)
            if probe_low < anchor:
                probe_sql = _raw_sql(state, probe_low, anchor, probe=True)
                probe_path = archive.root / (prefix + '-reference.arrow')
                prod.stream(probe_path, 'native', sql=probe_sql)
                archive.register(probe_path.name)
                archive.record(
                    prefix + '-reference-provenance.json',
                    {
                        'sql': probe_sql,
                        'window': [_stamp(_at(probe_low)), _stamp(_at(anchor))],
                        'max_rows': 1,
                        'held_state': 'held-state.json',
                        'native_values': 'unchanged',
                    },
                )
    return path.name


def _oracle_native(root: Path, name: str, case_id: str) -> Native:
    probe_path = root / (case_id + '-reference.arrow')
    if probe_path.exists():
        probe = _native(probe_path)
        if len(probe.ids) > 1:
            raise ValueError('Reference point probe returned more than one native row.')
        return _native(root / name, prefix=probe)
    return _native(root / name)


def _request(case: Mapping[str, object], state: Mapping[str, object]) -> dict[str, object]:
    return {
        'definition': case['definition'],
        'analysis': case['analysis'],
        'expected_state': {
            'data_cutoff': state['data_cutoff'],
            'pack_pin_digest': state['pack_pin_digest'],
        },
    }


def _download(prod: Production, archive: Archive, result_id: str) -> dict[str, str]:
    UUID(result_id)
    paths: dict[str, str] = {}
    for name in ('rallies.arrow', 'rally_cells.arrow', 'summary.arrow'):
        local = result_id + '-' + name
        prod.stream(archive.root / local, 'file', result_id=result_id, name=name)
        archive.register(local)
        paths[name] = local
    metadata = [_metadata(archive.root / name) for name in paths.values()]
    if not all(value == metadata[0] for value in metadata[1:]):
        raise ValueError('Canonical trio metadata differs.')
    return paths


def _latencies(samples: Sequence[Mapping[str, object]]) -> dict[str, object]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for sample in samples:
        grouped[str(sample['case_id'])].append(_float(sample['seconds']))
    return {
        key: {
            'p50_seconds': _rank(values, 0.5),
            'p95_seconds': _rank(values, 0.95),
            'max_seconds': max(values),
            'rounds': len(values),
        }
        for key, values in grouped.items()
    }


def _sample(
    prod: Production,
    archive: Archive,
    case: Mapping[str, object],
    state: Mapping[str, object],
    round_name: str,
) -> dict[str, object]:
    started = _stamp(datetime.now(UTC))
    before = time.perf_counter()
    answer = prod.read('post', route='rallies', body=_request(case, state), timeout=300)
    elapsed = time.perf_counter() - before
    name = f'{case["id"]}-{round_name}-response.json'
    archive.record(
        name,
        {
            'case_id': case['id'],
            'round': round_name,
            'started_at': started,
            'finished_at': _stamp(datetime.now(UTC)),
            'seconds': elapsed,
            'request': _request(case, state),
            'answer': answer,
        },
    )
    if answer['status'] != 200:
        raise ValueError(f'Authentic required case {case["id"]} {round_name} refused: {answer}.')
    body = _object(answer['body'])
    return {
        'case_id': case['id'],
        'round': round_name,
        'response': name,
        'result_id': body['result_id'],
        'seconds': elapsed,
        'files': _download(prod, archive, str(body['result_id'])),
    }


def _statement_sql(start: str, end: str, *, ids: Sequence[str] = ()) -> str:
    identifiers = ','.join(f"'{UUID(value)}'" for value in ids)
    condition = (
        f'log_comment IN ({identifiers})' if identifiers else "match(log_comment,'^[0-9a-f-]{36}$')"
    )
    return (
        'SELECT log_comment,toString(type) AS type,query_start_time_microseconds,event_time_microseconds,'
        'query_duration_ms,read_rows,read_bytes,result_rows,result_bytes,memory_usage,Settings AS settings,'
        "ProfileEvents['ExternalAggregationWritePart']+ProfileEvents['ExternalSortWritePart']+"
        "ProfileEvents['ExternalProcessingFilesTotal'] AS spill_events,query "
        'FROM system.query_log '
        f"WHERE event_time_microseconds >= parseDateTime64BestEffort('{start}',6,'UTC') "
        f"AND query_start_time_microseconds < parseDateTime64BestEffort('{end}',6,'UTC') "
        f'AND {condition} ORDER BY query_start_time_microseconds,event_time_microseconds'
    )


def _log_sql(start: str, end: str) -> str:
    return (
        'SELECT timestamp,service,container,message FROM origo.container_log '
        "WHERE service='market-state' "
        f"AND timestamp >= parseDateTime64BestEffort('{start}',3,'UTC') "
        f"AND timestamp < parseDateTime64BestEffort('{end}',3,'UTC') ORDER BY timestamp"
    )


def _source_capacity(prod: Production) -> dict[str, object]:
    return prod.query(
        'SELECT source_key,max(working_set_bytes) AS working_set_bytes '
        'FROM origo.source_capacity_log GROUP BY source_key ORDER BY source_key'
    )


def _receipt_sql(start: str, end: str) -> str:
    return (
        'SELECT feed,series,minute,recorded_at,status,error_code,error,rows,sha256,duration_ms '
        'FROM origo.worker_minute_log '
        f"WHERE recorded_at >= parseDateTime64BestEffort('{start}',3,'UTC') "
        f"AND recorded_at < parseDateTime64BestEffort('{end}',3,'UTC') ORDER BY recorded_at"
    )


def _receipts(prod: Production, start: str, end: str) -> dict[str, object]:
    return prod.query(_receipt_sql(start, end))


def _workload_failure(feed: object, status: object) -> bool:
    return status == 'FAILED' and (
        feed == 'market_state_api' or any(feed == source_feed for source_feed, _, _ in FEEDS)
    )


class ResourceSampler:
    def __init__(self, prod: Production, path: Path) -> None:
        self.prod, self.path = prod, path
        self.stop = threading.Event()
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        try:
            with self.path.open('xb') as handle:
                while not self.stop.is_set():
                    snapshot = self.prod.read('snapshot')
                    for key in ('results', 'files'):
                        del snapshot[key]
                    handle.write(_json(snapshot) + b'\n')
                    handle.flush()
                    self.stop.wait(2)
                os.fsync(handle.fileno())
        except (OSError, ValueError, subprocess.SubprocessError, RuntimeError) as error:
            self.error = error

    def start(self) -> None:
        self.thread.start()

    def finish(self) -> None:
        self.stop.set()
        self.thread.join(timeout=330)
        if self.thread.is_alive():
            raise RuntimeError('Resource evidence collection did not finish.')
        if self.error is not None:
            raise RuntimeError('Resource evidence acquisition failed.') from self.error


def _projection(root: Path, sample: Mapping[str, object]) -> dict[str, object]:
    files = _object(sample['files'])
    events = list(_rows(root / str(files['rallies.arrow'])))
    metadata = _metadata(root / str(files['rallies.arrow']))
    analysis = _object(metadata['analysis'])
    origin, ceiling = _us(_utc(analysis['start'])), _us(_utc(metadata['observation_ceiling']))
    actions: list[dict[str, object]] = []
    for number in range(LIMITS['local_actions']):
        n, m = number % 13, (number // 13) % 8
        known = origin + (ceiling - origin) * (number % 17 + 1) // 17
        eligible = {
            str(event['rally_id'])
            for event in events
            if _us(_utc(event['confirmed_at'])) < known
            and _float(event['duration_seconds']) >= number % 4
            and _float(event['volume']) >= number % 7
            and (
                event['anchor_at'] is None
                or _float(event['duration_seconds']) < (number % 240 + 1) * 60
            )
        }
        selected = min(eligible) if eligible else None
        union: set[tuple[int, int]] = set()
        contributions: dict[tuple[int, int], list[float | int]] = {}
        started = time.perf_counter()
        for row in _rows(root / str(files['rally_cells.arrow'])):
            if str(row['rally_id']) in eligible:
                key = (
                    _integer(row['base_time_index']) >> n,
                    _integer(row['base_price_index']) >> m,
                )
                union.add(key)
                if row['rally_id'] == selected:
                    value = contributions.setdefault(key, [0.0, 0, 0.0, 0])
                    for offset, measure in enumerate(
                        ('volume', 'trade_count', 'taker_buy_volume', 'taker_buy_trade_count')
                    ):
                        value[offset] += cast(float | int, row[measure])
        actions.append(
            {
                'number': number,
                'n': n,
                'm': m,
                'known_at': _stamp(_at(known)),
                'eligible_count': len(eligible),
                'selected': selected,
                'union_count': len(union),
                'selected_cell_count': len(contributions),
                'sha256': hashlib.sha256(
                    _json([sorted(union), sorted(contributions.items())])
                ).hexdigest(),
                'seconds': time.perf_counter() - started,
            }
        )
    return {
        'result_id': sample['result_id'],
        'actions': actions,
        'implementation': 'Independent streamed canonical Arrow reducer; no service imports or network requests.',
    }


def _reuse(prod: Production, archive: Archive, sample: Mapping[str, object]) -> str:
    before = prod.read('snapshot')
    archive.record('reuse-before.json', before)
    projection = _projection(archive.root, sample)
    after = prod.read('snapshot')
    archive.record('reuse-after.json', after)
    archive.record('reuse-projections.json', projection)
    # Existing query/log collectors are eventually ingested. Do not flush production logs.
    time.sleep(30)
    archive.record(
        'reuse-statements.json', prod.query(_statement_sql(str(before['at']), str(after['at'])))
    )
    archive.record(
        'reuse-container-logs.json', prod.query(_log_sql(str(before['at']), str(after['at'])))
    )
    return archive.record(
        'reuse.json',
        {
            'before': 'reuse-before.json',
            'after': 'reuse-after.json',
            'projection': 'reuse-projections.json',
            'statements': 'reuse-statements.json',
            'logs': 'reuse-container-logs.json',
            'settle_seconds': 30,
        },
    )


def _concurrency(
    prod: Production,
    archive: Archive,
    case: Mapping[str, object],
    state: Mapping[str, object],
    sample: Mapping[str, object],
) -> str:
    start = str(prod.read('snapshot')['at'])
    ordinary: dict[str, object] = {
        't1': '2026-06-25T00:00:00Z',
        't2': '2026-06-27T00:00:00Z',
        'tR': 900,
        'pR': 1000,
    }
    renewals: list[dict[str, object]] = []
    paths = _object(sample['files'])
    metadata = _metadata(archive.root / str(paths['rallies.arrow']))
    path = '/opt/origo/market-state/results/' + str(sample['result_id']) + '/rallies.arrow'
    with ThreadPoolExecutor(max_workers=2) as pool:
        discovery = pool.submit(
            prod.read, 'post', route='rallies', body=_request(case, state), timeout=300
        )
        # Observe an actual tagged statement before issuing the competing requests.
        deadline = time.monotonic() + 30
        active: list[dict[str, object]] = []
        while time.monotonic() < deadline and not discovery.done():
            active = _records(
                prod.query(
                    "SELECT query_id,elapsed,log_comment FROM system.processes WHERE match(log_comment,'^[0-9a-f-]{36}$')"
                )
            )
            if active:
                break
            time.sleep(0.2)
        archive.record('concurrency-observed-active.json', active)
        if not active:
            raise ValueError('No authentic in-flight discovery observed for concurrency proof.')
        query = pool.submit(prod.read, 'post', route='query', body=ordinary, timeout=300)
        second = prod.read('post', route='rallies', body=_request(case, state), timeout=300)
        while not discovery.done():
            renewals.append(prod.read('reader', path=path))
            time.sleep(0.1)
        rally_answer, query_answer = discovery.result(), query.result()
    end = str(prod.read('snapshot')['at'])
    time.sleep(30)
    statements = archive.record(
        'concurrency-statements.json', prod.query(_statement_sql(start, end))
    )
    return archive.record(
        'concurrency.json',
        {
            'start': start,
            'end': end,
            'rally': rally_answer,
            'ordinary': query_answer,
            'second_rally': second,
            'renewals': renewals,
            'renewed_result_id': sample['result_id'],
            'renewed_metadata': metadata,
            'statements': statements,
            'observed_active': 'concurrency-observed-active.json',
        },
    )


def _merged_deployment(prod: Production, archive: Archive) -> dict[str, object]:
    since = datetime.now(UTC) - timedelta(minutes=30)
    dagit_query = (
        '{ assetOrError(assetKey:{path:["market_state_query_service"]}) { ... on Asset { assetMaterializations(afterTimestampMillis:"'
        + str(int(since.timestamp() * 1000))
        + '",limit:10000) { timestamp metadataEntries { label ... on IntMetadataEntry { intValue } } } } } }'
    )
    dagit = prod.read('dagit', query=dagit_query)
    archive.record('dagit-preflight.json', dagit)
    if 'errors' in dagit or not _list(
        _object(_object(_object(dagit['data'])['assetOrError']))['assetMaterializations']
    ):
        raise ValueError('Dagit has no actual query-service materializations for the baseline.')
    evidence = prod.read('preflight', files=list(SOURCE_FILES))
    archive.record('deployment.json', evidence)
    deployment = _object(evidence['deployment'])
    match = re.search(r':([0-9a-f]{40})$', str(deployment['image']))
    if match is None:
        raise ValueError('Deployed image lacks an immutable Git merge SHA.')
    sha = match[1]
    repository = Path(__file__).resolve().parents[1]
    subprocess.run(
        ['git', 'fetch', 'origin', 'main'], cwd=repository, check=True, capture_output=True
    )
    subprocess.run(
        ['git', 'merge-base', '--is-ancestor', sha, 'origin/main'],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    hashes = _object(evidence['source_files'])
    for name in SOURCE_FILES:
        committed = subprocess.check_output(['git', 'show', f'{sha}:{name}'], cwd=repository)
        if (
            hashlib.sha256(committed).hexdigest() != hashes[name]
            or _sha(repository / name) != hashes[name]
        ):
            raise ValueError(f'{name}: local/deployed/merged implementation identities differ.')
    if (
        deployment['memory'] != LIMITS['container_bytes']
        or deployment['nanocpus'] != LIMITS['container_nanocpus']
    ):
        raise ValueError('Deployed container differs from the frozen 2 CPU / 2 GiB envelope.')
    snapshot = _object(evidence['snapshot'])
    observed = {str(_object(value)['name']) for value in _list(snapshot['heartbeats'])}
    missing = set(_required_heartbeats()) - observed
    if missing:
        raise ValueError('Required deployed worker heartbeats are missing: ' + ', '.join(sorted(missing)))
    return {**evidence, 'merge_sha': sha}


def _legacy(archive: Archive, merge_sha: str) -> str:
    repository = Path(__file__).resolve().parents[1]
    fixtures = repository / 'tests/origo_source_native/fixtures/binance_rallies'
    inventory = {
        str(path.relative_to(repository)): _sha(path)
        for path in (
            *fixtures.iterdir(),
            *(repository / 'tests/fixtures/binance/spot/daily/trades/revisioned').glob(
                'BTCUSDT-trades-2017-08-17*'
            ),
        )
        if path.is_file()
    }
    for relative, expected_hash in inventory.items():
        committed = subprocess.check_output(
            ['git', 'show', f'{merge_sha}:{relative}'], cwd=repository
        )
        if hashlib.sha256(committed).hexdigest() != expected_hash:
            raise ValueError('Legacy fixture differs from the unchanged merged capture.')
    result = subprocess.run(
        [
            sys.executable,
            '-m',
            'pytest',
            'tests/origo_source_native/test_rally_detection.py::test_legacy_r30v1_outputs_match_baseline',
            '-q',
        ],
        cwd=repository,
        capture_output=True,
        timeout=600,
    )
    return archive.record(
        'legacy.json',
        {
            'command': 'pytest tests/origo_source_native/test_rally_detection.py::test_legacy_r30v1_outputs_match_baseline -q',
            'exit_code': result.returncode,
            'stdout': result.stdout.decode(),
            'stderr': result.stderr.decode(),
            'fixture_sha256': inventory,
        },
    )


def acceptance(url: str, report_path: Path) -> int:
    if report_path.exists() or report_path.with_suffix(report_path.suffix + '.sha256').exists():
        raise FileExistsError('Acceptance reports are immutable; choose a new report path.')
    archive = Archive(report_path)
    report: dict[str, object] = {
        'schema_version': SCHEMA_VERSION,
        'status': 'FAIL',
        'manifest': 'manifest.json',
        'samples': [],
        'oracles': [],
    }
    prod = Production(url)
    sampler: ResourceSampler | None = None
    try:
        deployment = _merged_deployment(prod, archive)
        state = prod.read('pin')
        archive.record('held-state.json', state)
        manifest = {
            **frozen_manifest(),
            'held_state': 'held-state.json',
            'deployment': 'deployment.json',
            'merge_sha': deployment['merge_sha'],
            'frozen_at': _stamp(datetime.now(UTC)),
        }
        archive.record('manifest.json', manifest)
        report['manifest_sha256'] = _sha(archive.root / 'manifest.json')
        archive.record('source-capacity-before.json', _source_capacity(prod))
        before = prod.read('snapshot')
        archive.record('before.json', before)
        started = _utc(before['at'])
        archive.record(
            'baseline-receipts.json',
            _receipts(prod, _stamp(started - timedelta(minutes=36)), _stamp(started)),
        )
        sampler = ResourceSampler(prod, archive.root / 'resources.jsonl')
        sampler.start()
        samples: list[dict[str, object]] = []
        oracles: list[dict[str, object]] = []
        for case_value in _list(manifest['cases']):
            case = _object(case_value)
            case_samples = [
                _sample(prod, archive, case, state, str(round_name))
                for round_name in _list(manifest['rounds'])
            ]
            samples.extend(case_samples)
            report['samples'] = samples
            native_name = _capture_native(prod, archive, state, case)
            raw = _oracle_native(archive.root, native_name, str(case['id']))
            paths = _object(case_samples[0]['files'])
            comparison = oracle(
                raw,
                case,
                archive.root / str(paths['rallies.arrow']),
                archive.root / str(paths['rally_cells.arrow']),
            )
            comparison['native'] = native_name
            comparison['sample_result_id'] = case_samples[0]['result_id']
            name = archive.record(str(case['id']) + '-oracle.json', comparison)
            oracles.append({'case_id': case['id'], 'evidence': name})
            report['oracles'] = oracles
            hashes = comparison['content_hashes']
            for sample in case_samples[1:]:
                paths = _object(sample['files'])
                warm = oracle(
                    raw,
                    case,
                    archive.root / str(paths['rallies.arrow']),
                    archive.root / str(paths['rally_cells.arrow']),
                )
                if warm['content_hashes'] != hashes:
                    raise ValueError('Immutable published event content changed between rounds.')
            print(f'{case["id"]}: measured 6 rounds, exact native oracle verified.', flush=True)
        proof_samples: list[dict[str, object]] = []
        for case_value in _list(manifest['proof_cases']):
            case = _object(case_value)
            sample = _sample(prod, archive, case, state, 'proof')
            proof_samples.append(sample)
            if str(case['id']).startswith('positive_'):
                name = _capture_native(prod, archive, state, case)
                paths = _object(sample['files'])
                comparison = oracle(
                    _oracle_native(archive.root, name, str(case['id'])),
                    case,
                    archive.root / str(paths['rallies.arrow']),
                    archive.root / str(paths['rally_cells.arrow']),
                )
                comparison['native'] = name
                comparison['sample_result_id'] = sample['result_id']
                archive.record(str(case['id']) + '-oracle.json', comparison)
        report['proof_samples'] = proof_samples
        report['latencies'] = _latencies(samples)
        report['reuse'] = _reuse(prod, archive, samples[0])
        longest = _object(_list(manifest['cases'])[-1])
        report['concurrency'] = _concurrency(prod, archive, longest, state, samples[0])
        revisions = prod.query(
            "SELECT count() AS multi_revision_partitions FROM (SELECT partition_key FROM origo.source_activation_log WHERE source_key='binance_spot_trades' GROUP BY partition_key HAVING uniqExact(revision)>1)"
        )
        archive.record(
            'corrected-revision.json',
            {
                'status': 'deferred',
                'reason': _object(manifest['corrected_revision'])['reason'],
                'production_evidence': revisions,
            },
        )
        report['corrected_revision'] = 'corrected-revision.json'
        report['legacy'] = _legacy(archive, str(manifest['merge_sha']))
        # Actual after-workload feed, coverage, heartbeat and capacity evidence is mandatory.
        ending = time.monotonic() + LIMITS['ingestion_seconds'] + 360
        while time.monotonic() < ending:
            time.sleep(min(30, ending - time.monotonic()))
        after = prod.read('snapshot')
        archive.record('after.json', after)
        archive.record('after-held-state.json', prod.read('pin'))
        archive.record('source-capacity-after.json', _source_capacity(prod))
        archive.record(
            'after-receipts.json',
            _receipts(prod, _stamp(_utc(after['at']) - timedelta(minutes=36)), str(after['at'])),
        )
        archive.record(
            'workload-receipts.json', _receipts(prod, str(before['at']), str(after['at']))
        )
        ids = [str(sample['result_id']) for sample in (*samples, *proof_samples)]
        archive.record(
            'statements.json',
            prod.query(_statement_sql(str(before['at']), str(after['at']), ids=ids)),
        )
        archive.record(
            'container-logs.json', prod.query(_log_sql(str(before['at']), str(after['at'])))
        )
        sampler.finish()
        sampler = None
        archive.register('resources.jsonl')
        report['evidence'] = archive.entries
        report['failures'] = validate_report(report, manifest, archive.root)
        report['status'] = 'PASS' if not report['failures'] else 'FAIL'
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
        report['failure'] = f'{type(error).__name__}: {error}'
        if sampler is not None:
            try:
                sampler.finish()
            except RuntimeError as resource_error:
                report['resource_failure'] = str(resource_error)
            if (archive.root / 'resources.jsonl').exists():
                archive.register('resources.jsonl')
    archive.finish(report)
    print(report['status'])
    return 0 if report['status'] == 'PASS' else 1


def _validate_resources(root: Path, measured: Mapping[str, int]) -> list[str]:
    failures: list[str] = []
    before = _object(json.loads((root / 'before.json').read_bytes()))
    events_before = str(_object(before['cgroup'])['memory.events'])
    old_events = {line.split()[0]: int(line.split()[1]) for line in events_before.splitlines()}
    observed_heartbeats = {str(_object(value)['name']) for value in _list(before['heartbeats'])}
    required_heartbeats = set(_required_heartbeats())
    expected_heartbeats = required_heartbeats | observed_heartbeats
    if not required_heartbeats <= observed_heartbeats:
        failures.append('Required real worker heartbeats are unavailable.')
    snapshots = 0
    with (root / 'resources.jsonl').open() as handle:
        for line in handle:
            snapshot = _object(json.loads(line))
            snapshots += 1
            info, rss, cgroup = (_object(snapshot[key]) for key in ('container', 'rss', 'cgroup'))
            if (
                info['memory'] != LIMITS['container_bytes']
                or info['nanocpus'] != LIMITS['container_nanocpus']
            ):
                failures.append('Container resource envelope changed during measurements.')
            if max(_integer(rss['VmRSS']), _integer(rss['VmHWM'])) > LIMITS['worker_rss_bytes']:
                failures.append('Worker RSS exceeds 1.5 GiB.')
            if int(str(cgroup['memory.current'])) >= LIMITS['container_bytes'] or str(
                cgroup['memory.max']
            ) != str(LIMITS['container_bytes']):
                failures.append('Cgroup memory admission or limit breached.')
            events = {
                part.split()[0]: int(part.split()[1])
                for part in str(cgroup['memory.events']).splitlines()
            }
            if info['oom_killed'] or any(
                events[key] > old_events[key] for key in ('max', 'oom', 'oom_kill')
            ):
                failures.append('Container memory/OOM event occurred.')
            stored, reserved = (
                _integer(snapshot['stored_bytes']),
                _integer(snapshot['reserved_bytes']),
            )
            if stored + reserved > LIMITS['store_bytes']:
                failures.append('Global result storage/reservation budget exceeded.')
            capacity = max(
                (_integer(snapshot['total_bytes']) * CAPACITY_TOTAL_RESERVE_TENTHS + 9) // 10,
                *(
                    measured.get(spec.key, 0)
                    * CAPACITY_WORKING_SET_FACTOR
                    * spec.orchestration.canonical_concurrency
                    for spec in SOURCE_REGISTRY
                ),
            )
            if _integer(snapshot['staging_count']) > 2:
                failures.append('More than two heavy query reservations observed.')
            if _integer(snapshot['free_bytes']) - _integer(
                snapshot['pending_bytes']
            ) < capacity + LIMITS['disk_margin_bytes'] or _integer(
                snapshot['free_inodes']
            ) * 10 < _integer(snapshot['total_inodes']):
                failures.append('Source working reserve/disk floor breached.')
            seen = {
                str(_object(item)['name']): _float(_object(item)['mtime'])
                for item in _list(snapshot['heartbeats'])
            }
            at = _utc(snapshot['at']).timestamp()
            if not expected_heartbeats <= seen.keys() or any(
                at - seen[name] > 180 for name in expected_heartbeats
            ):
                failures.append('A real worker heartbeat is missing/stale.')
    if snapshots < 2:
        failures.append('Missing continuous container/resource/heartbeat observations.')
    return sorted(set(failures))


def _validate_statements(evidence: Mapping[str, object], ids: set[str]) -> list[str]:
    failures: list[str] = []
    rows = _records(evidence)
    finished = [row for row in rows if row['type'] == 'QueryFinish']
    if not ids <= {str(row['log_comment']) for row in finished}:
        failures.append('Actual statement evidence is missing for a successful result.')
    for row in rows:
        if row['type'] not in ('QueryStart', 'QueryFinish'):
            failures.append('An acceptance statement failed.')
        if row['type'] == 'QueryStart':
            continue
        settings = _object(row['settings'])
        required = {
            'max_threads': LIMITS['statement_threads'],
            'max_memory_usage': LIMITS['statement_bytes'],
            'max_execution_time': LIMITS['statement_seconds'],
        }
        for key, maximum in required.items():
            if key not in settings or not 0 < float(str(settings[key])) <= maximum:
                failures.append(f'Unverified/exceeded statement {key}.')
        for key in (
            'max_bytes_ratio_before_external_group_by',
            'max_bytes_ratio_before_external_sort',
        ):
            if key not in settings or float(str(settings[key])) != 0:
                failures.append('Statement spill policy is unverified.')
        if (
            _integer(row['spill_events']) != 0
            or _integer(row['memory_usage']) > LIMITS['statement_bytes']
        ):
            failures.append('Statement spill or memory threshold breached.')
        if _integer(row['query_duration_ms']) > LIMITS['statement_seconds'] * 1000:
            failures.append('Statement duration threshold breached.')
    return sorted(set(failures))


def _validate_reuse(root: Path) -> list[str]:
    failures: list[str] = []
    before = _object(json.loads((root / 'reuse-before.json').read_bytes()))
    after = _object(json.loads((root / 'reuse-after.json').read_bytes()))
    projection = _object(json.loads((root / 'reuse-projections.json').read_bytes()))
    result_id = str(UUID(str(projection['result_id'])))
    derived = _projection(
        root,
        {
            'result_id': result_id,
            'files': {
                name: result_id + '-' + name for name in ('rallies.arrow', 'rally_cells.arrow')
            },
        },
    )
    actions = [_object(value) for value in _list(projection['actions'])]
    expected_actions = [_object(value) for value in _list(derived['actions'])]
    if len(actions) != LIMITS['local_actions']:
        failures.append('Exactly 200 authentic local projection changes were not completed.')
    if len(actions) == len(expected_actions) and any(
        {key: value for key, value in observed.items() if key != 'seconds'}
        != {key: value for key, value in expected.items() if key != 'seconds'}
        for observed, expected in zip(actions, expected_actions, strict=True)
    ):
        failures.append('Local projection parameters/outputs differ from retained canonical evidence.')
    if before['results'] != after['results'] or any(
        before[key] != after[key]
        for key in ('stored_bytes', 'reserved_bytes', 'result_bytes', 'staging_bytes')
    ):
        failures.append('Local view changes altered result IDs or result-store usage.')
    statements = _records(_object(json.loads((root / 'reuse-statements.json').read_bytes())))
    if statements:
        failures.append('A tagged Origo ClickHouse statement occurred during local reuse.')
    logs = _records(_object(json.loads((root / 'reuse-container-logs.json').read_bytes())))
    if any(
        'detector_call' in str(row['message'])
        or 'discovery_post' in str(row['message'])
        or ' published ' in str(row['message'])
        for row in logs
    ):
        failures.append('Server discovery/detector/publication occurred during local reuse.')
    return failures


def _validate_concurrency(root: Path) -> list[str]:
    failures: list[str] = []
    proof = _object(json.loads((root / 'concurrency.json').read_bytes()))
    rally, ordinary, second = (_object(proof[key]) for key in ('rally', 'ordinary', 'second_rally'))
    if (
        rally['status'] != 200
        or ordinary['status'] != 200
        or second['status'] != 503
        or _object(second['body']).get('error') != 'busy'
    ):
        failures.append('Required concurrent discovery/query/second-rally outcomes differ.')
    if set(_object(ordinary['body'])) != {
        'result_id',
        'cells',
        'summary',
        'expires_after_seconds',
        'expires_at',
        'effective',
        'clipped',
        'data_cutoff',
        'canonical_through',
        'last_column_unfinished',
        'state_token',
        'cell_count',
    }:
        failures.append('Ordinary cube response contract changed.')
    renewals = [_object(value) for value in _list(proof['renewals'])]
    if (
        not renewals
        or _rank([_float(value['seconds']) for value in renewals], 0.95)
        > LIMITS['access_p95_seconds']
    ):
        failures.append('Actual supported-reader renewal p95 exceeds 1 second or is unproved.')
    rows = _records(_object(json.loads((root / str(proof['statements'])).read_bytes())))
    intervals: dict[str, tuple[datetime, datetime]] = {}
    for row in rows:
        if row['type'] == 'QueryFinish':
            key = str(row['log_comment'])
            start, end = (
                _utc(row['query_start_time_microseconds']),
                _utc(row['event_time_microseconds']),
            )
            previous = intervals.get(key)
            intervals[key] = (
                (min(start, previous[0]), max(end, previous[1])) if previous else (start, end)
            )
    ids = [str(_object(answer['body']).get('result_id', '')) for answer in (rally, ordinary)]
    if any(key not in intervals for key in ids):
        failures.append('Actual overlapping query-log intervals are missing.')
    else:
        first, other = (intervals[key] for key in ids)
        if max(first[0], other[0]) >= min(first[1], other[1]):
            failures.append('Discovery and external ordinary query did not actually overlap.')
    active_count = 0
    for _, change in sorted(
        (moment, delta)
        for low, high in intervals.values()
        for moment, delta in ((low, 1), (high, -1))
    ):
        active_count += change
        if active_count > 2:
            failures.append('More than two concurrent tagged heavy requests observed.')
    failures.extend(
        _validate_statements(
            _object(json.loads((root / str(proof['statements'])).read_bytes())), set(ids)
        )
    )
    return failures


def _feed_window(
    rows: Sequence[Mapping[str, object]], bounds: tuple[datetime, datetime]
) -> dict[str, object]:
    start, end = bounds
    minutes = tuple(start + timedelta(minutes=index) for index in range(30))
    results: dict[str, object] = {}
    for feed, series, mount in FEEDS:
        source = [row for row in rows if row['feed'] == feed and row['series'] == series]
        publications = (
            sorted(
                _utc(row['recorded_at'])
                for row in rows
                if row['feed'] == feed and row['series'] == mount and row['status'] == 'OK'
            )
            if mount
            else []
        )
        landed: dict[datetime, datetime] = {}
        for row in source:
            if row['status'] == 'OK':
                minute, at = _utc(row['minute']), _utc(row['recorded_at'])
                landed[minute] = min(landed.get(minute, at), at)
        lags: list[float] = []
        publication_lags: list[float] = []
        missing = 0
        for minute in minutes:
            at = landed.get(minute)
            if at is None:
                missing += 1
            else:
                lags.append((at - (minute + timedelta(minutes=1))).total_seconds())
                if mount:
                    published = next((at_pub for at_pub in publications if at_pub >= at), None)
                    if published is None:
                        missing += 1
                    else:
                        publication_lags.append(
                            (published - (minute + timedelta(minutes=1))).total_seconds()
                        )
        failed = sum(
            row['status'] == 'FAILED' and start <= _utc(row['recorded_at']) < end for row in source
        )
        results[f'{feed}/{series}'] = {
            'missing': missing,
            'failed': failed,
            'landing_lags': lags,
            'publication_lags': publication_lags,
        }
    return results


def _validate_ingestion(root: Path) -> list[str]:
    before = _object(json.loads((root / 'before.json').read_bytes()))
    after = _object(json.loads((root / 'after.json').read_bytes()))
    start, end = _utc(before['at']), _utc(after['at'])
    if (end - start).total_seconds() < LIMITS['ingestion_seconds']:
        return ['Actual 30-minute after-workload evidence is missing.']
    baseline_rows = _records(_object(json.loads((root / 'baseline-receipts.json').read_bytes())))
    after_rows = _records(_object(json.loads((root / 'after-receipts.json').read_bytes())))
    baseline_edge = (start - timedelta(minutes=5)).replace(second=0, microsecond=0)
    after_edge = (end - timedelta(minutes=5)).replace(second=0, microsecond=0)
    baseline = _feed_window(baseline_rows, (baseline_edge - timedelta(minutes=30), baseline_edge))
    actual = _feed_window(after_rows, (after_edge - timedelta(minutes=30), after_edge))
    failures: list[str] = []
    workload = _object(json.loads((root / 'workload-receipts.json').read_bytes()))
    if workload['sql'] != _receipt_sql(str(before['at']), str(after['at'])):
        failures.append('Workload receipt evidence does not cover the complete observed interval.')
    for row in _records(workload):
        if not start <= _utc(row['recorded_at']) < end:
            failures.append('Workload receipt evidence contains an out-of-window observation.')
        if _workload_failure(row['feed'], row['status']):
            failures.append(f'{row["feed"]}/{row["series"]}: FAILED receipt during the workload.')
    for name in baseline:
        reference, observed = _object(baseline[name]), _object(actual[name])
        if reference['missing'] or observed['missing'] or observed['failed']:
            failures.append(f'{name}: missing receipts/publication or failed ingestion.')
        for key in ('landing_lags', 'publication_lags'):
            left, right = (
                [_float(value) for value in _list(reference[key])],
                [_float(value) for value in _list(observed[key])],
            )
            if bool(left) != bool(right) or (left and _rank(right, 0.95) > _rank(left, 0.95)):
                failures.append(f'{name}: ingestion backlog worsened.')
    state_before = _object(json.loads((root / 'held-state.json').read_bytes()))
    state_after = _object(json.loads((root / 'after-held-state.json').read_bytes()))
    lag_before = (start - _utc(state_before['data_cutoff'])).total_seconds()
    lag_after = (end - _utc(state_after['data_cutoff'])).total_seconds()
    if lag_after > lag_before:
        failures.append('Actual accepted cube coverage backlog worsened.')
    return failures


def validate_report(
    report: Mapping[str, object],
    manifest: Mapping[str, object] | None = None,
    evidence_root: Path | None = None,
) -> list[str]:
    """Derive failures from immutable observations; a caller's PASS flag proves nothing."""
    failures: list[str] = []
    if evidence_root is None or manifest is None:
        return ['Manifest and immutable evidence directory are required.']
    try:
        frozen = frozen_manifest()
        if any(manifest.get(key) != value for key, value in frozen.items()):
            failures.append('Frozen corpus/settings/tolerance/deferral policy differs.')
        if report.get('schema_version') != SCHEMA_VERSION:
            failures.append('Report schema version is missing or unsupported.')
        if report.get('failure'):
            failures.append(str(report['failure']))
        inventory = [_object(value) for value in _list(report['evidence'])]
        names = {str(entry['file']) for entry in inventory}
        if len(names) != len(inventory):
            failures.append('Evidence inventory contains duplicate file names.')
        for entry in inventory:
            name = str(entry['file'])
            if Path(name).name != name:
                raise ValueError('Evidence paths must be local sibling file names.')
            path = evidence_root / name
            if (
                not path.is_file()
                or _sha(path) != entry['sha256']
                or path.stat().st_size != entry['bytes']
            ):
                failures.append(f'{name}: missing or altered immutable evidence.')
        if {path.name for path in evidence_root.iterdir() if path.is_file()} != names:
            failures.append('Immutable evidence inventory does not cover exactly the saved files.')
        if failures:
            return failures
        required = {
            'manifest.json',
            'held-state.json',
            'deployment.json',
            'dagit-preflight.json',
            'source-capacity-before.json',
            'source-capacity-after.json',
            'resources.jsonl',
            'before.json',
            'after.json',
            'after-held-state.json',
            'baseline-receipts.json',
            'after-receipts.json',
            'workload-receipts.json',
            'statements.json',
            'container-logs.json',
            'legacy.json',
            'corrected-revision.json',
            'reuse.json',
            'reuse-before.json',
            'reuse-after.json',
            'reuse-statements.json',
            'reuse-container-logs.json',
            'reuse-projections.json',
            'concurrency.json',
            'concurrency-statements.json',
            'concurrency-observed-active.json',
        }
        if not required <= names:
            failures.append('Missing required evidence: ' + ', '.join(sorted(required - names)))
            return failures
        if _sha(evidence_root / 'manifest.json') != report['manifest_sha256'] or _object(
            json.loads((evidence_root / 'manifest.json').read_bytes())
        ) != dict(manifest):
            failures.append('Manifest commitment differs.')
        deployment = _object(json.loads((evidence_root / 'deployment.json').read_bytes()))
        deployed = _object(deployment['deployment'])
        merge_sha = str(manifest['merge_sha'])
        if not re.fullmatch(r'[0-9a-f]{40}', merge_sha) or not str(deployed['image']).endswith(
            ':' + merge_sha
        ):
            failures.append('Immutable deployed merge identity is unverified.')
        if (
            deployed['memory'] != LIMITS['container_bytes']
            or deployed['nanocpus'] != LIMITS['container_nanocpus']
        ):
            failures.append('Deployment resource envelope was enlarged or changed.')
        state = _object(json.loads((evidence_root / 'held-state.json').read_bytes()))
        pairs = sorted(
            (str(_object(value)['key']), [_object(value)['revision'], _object(value)['build_id']])
            for value in _list(state['records'])
        )
        commitment = hashlib.sha256(
            json.dumps([state['data_cutoff'], pairs], separators=(',', ':')).encode()
        ).hexdigest()
        if state['pack_pin_digest'] != commitment:
            failures.append('Held cube commitment does not match actual pinned records.')
        cases = {str(_object(value)['id']): _object(value) for value in _list(manifest['cases'])}
        proof_cases = {
            str(_object(value)['id']): _object(value) for value in _list(manifest['proof_cases'])
        }
        expected = {
            (name, str(round_name)) for name in cases for round_name in _list(manifest['rounds'])
        }
        samples = [_object(value) for value in _list(report['samples'])]
        actual = {(str(sample['case_id']), str(sample['round'])) for sample in samples}
        if report.get('latencies') != _latencies(samples):
            failures.append('Recorded end-to-end p50/p95/max do not match measured rounds.')
        if actual != expected or len(samples) != len(expected):
            failures.append('Required 24 cases x 6 measured rounds are missing/duplicated/skipped.')
        proofs = [_object(value) for value in _list(report['proof_samples'])]
        if {str(sample['case_id']) for sample in proofs} != proof_cases.keys() or len(
            proofs
        ) != len(proof_cases):
            failures.append('Authentic boundary/positive proof cases are incomplete.')
        result_ids: set[str] = set()
        for sample in (*samples, *proofs):
            case_id = str(sample['case_id'])
            case = cases.get(case_id, proof_cases.get(case_id))
            if case is None:
                raise ValueError('Unfrozen discovery case.')
            response = _object(json.loads((evidence_root / str(sample['response'])).read_bytes()))
            if _utc(response['started_at']) <= _utc(manifest['frozen_at']):
                failures.append('Measurements preceded the immutable manifest.')
            answer = _object(response['answer'])
            body = _object(answer['body'])
            if answer['status'] != 200 or response['request'] != _request(case, state):
                failures.append(f'{case_id}: refused or mismatched frozen discovery.')
            rid = str(body['result_id'])
            UUID(rid)
            if rid != sample['result_id'] or rid in result_ids:
                failures.append('Missing/duplicated publication identity.')
            result_ids.add(rid)
            if _float(response['seconds']) > LIMITS['reader_seconds']:
                failures.append(f'{case_id}: end-to-end reader deadline exceeded.')
            bulk, probes = _object(body['bulk_read']), _object(body['reference_probe'])
            input_bytes = (_integer(bulk['rows']) + _integer(probes['returned_rows'])) * ROW_BYTES
            if input_bytes > LIMITS['input_bytes']:
                failures.append(f'{case_id}: native input admission threshold exceeded.')
            if (
                _integer(bulk['rows']) + _integer(probes['returned_rows']) > LIMITS['input_rows']
                or _integer(probes['returned_rows']) > 1
            ):
                failures.append(f'{case_id}: native row/point-probe bound exceeded.')
            if case_id in cases and {'start': bulk['start'], 'end': bulk['end']} != case['bulk']:
                failures.append(f'{case_id}: effective actual bulk bounds differ.')
            files = _object(sample['files'])
            if set(files) != {'rallies.arrow', 'rally_cells.arrow', 'summary.arrow'}:
                failures.append('Publication is not exactly the canonical trio.')
            if any(
                name != rid + '-' + key or str(name) not in names for key, name in files.items()
            ):
                raise ValueError('Unregistered/mismatched canonical file evidence.')
            output_bytes = sum(
                (evidence_root / str(name)).stat().st_size for name in files.values()
            )
            if output_bytes > LIMITS['output_bytes']:
                failures.append(f'{case_id}: canonical output admission threshold exceeded.')
            metadata = [_metadata(evidence_root / str(name)) for name in files.values()]
            if (
                not all(value == metadata[0] for value in metadata)
                or metadata[0]['result_id'] != rid
            ):
                failures.append('Canonical trio shared identities differ.')
            shared = metadata[0]
            analysis = _object(case['analysis'])
            origin, edge = (
                _us(_utc(analysis['start'])),
                min(_us(_utc(analysis['end'])), _us(_utc(state['data_cutoff']))),
            )
            bulk_low = (
                origin
                if _object(case['definition'])['scale'] == 'bps'
                else max(T0_US, origin // BAR_US * BAR_US - 15 * BAR_US)
            )
            reference_windows = [
                [_us(_utc(pair[0])), _us(_utc(pair[1]))]
                for pair in (_list(value) for value in _list(case['reference_windows']))
            ]
            base_low = T0_US + (origin - T0_US) // BASE_US * BASE_US
            base_high = min(
                T0_US + -(-(edge - T0_US) // BASE_US) * BASE_US, _us(_utc(state['data_cutoff']))
            )
            required_windows = [
                (bulk_low, edge),
                (base_low, base_high),
                *(tuple(pair) for pair in reference_windows),
            ]
            relevant = sorted(
                [record['key'], record['revision'], record['build_id']]
                for record in (_object(value) for value in _list(state['records']))
                if any(
                    _us(_utc(record['start'])) < high and _us(_utc(record['end'])) > low
                    for low, high in required_windows
                )
            )
            source_fields = {
                'bulk_read': [_stamp(_at(bulk_low)), _stamp(_at(edge))],
                'reference_windows': case['reference_windows'],
                'pins': relevant,
            }
            source_digest = hashlib.sha256(b'rally_source_v1\n' + _json(source_fields)).hexdigest()
            if (
                shared['relevant_pins'] != relevant
                or shared['relevant_pin_digest'] != source_digest
            ):
                failures.append('Relevant raw/context source identity differs from the held pack.')
            if (
                shared['normalized_definition'] != case['normalized_definition']
                or shared['source'] != 'binance_spot_trades'
                or shared['instrument'] != 'BTCUSDT'
            ):
                failures.append('Canonical definition/source capabilities differ.')
            if shared['observation_ceiling'] != _stamp(_at(edge)) or shared[
                'diagnostics_as_of'
            ] != _stamp(_at(edge)):
                failures.append('Canonical diagnostics or event observation ceiling differs.')
            summaries = list(_rows(evidence_root / str(files['summary.arrow'])))
            if len(summaries) != 1:
                failures.append('Canonical summary is not exactly one row.')
            else:
                _equal(
                    summaries[0],
                    {
                        'rally_count': body['rally_count'],
                        **_object(shared['diagnostic_counts']),
                        'diagnostics_as_of': _at(edge),
                    },
                    'summary',
                )
            if shared['pack_pin_digest'] != commitment or shared[
                'definition_fingerprint'
            ] != _fingerprint(_object(case['definition'])):
                failures.append(
                    'Canonical evidence does not bind the held definition/source state.'
                )
            if (
                shared['detector_build']
                != _object(deployment['source_files'])['origo/query/rally_detection.py']
            ):
                failures.append('Result detector build differs from deployed source.')
        features = {
            'partial': 0,
            'endpoint_overlap': 0,
            'outside_endpoint_prices': 0,
            'timestamp_ties': 0,
        }
        for case_id, case in {
            **cases,
            **{name: value for name, value in proof_cases.items() if name.startswith('positive_')},
        }.items():
            case_samples = [
                sample for sample in (*samples, *proofs) if sample['case_id'] == case_id
            ]
            if not case_samples:
                failures.append(f'{case_id}: no independent authentic oracle.')
                continue
            oracle_path = evidence_root / (case_id + '-oracle.json')
            captured = _object(json.loads(oracle_path.read_bytes()))
            provenance = _object(
                json.loads((evidence_root / (case_id + '-native-provenance.json')).read_bytes())
            )
            if (
                provenance['native_values'] != 'unchanged'
                or provenance['held_state'] != 'held-state.json'
            ):
                failures.append('Native oracle provenance is not authentic held source state.')
            low, high = _integer(provenance['low_us']), _integer(provenance['high_us'])
            if provenance['sql'] != _raw_sql(state, low, high):
                failures.append('Native oracle SQL differs from frozen pinned reads.')
            if (
                captured['native'] != case_id + '-native.arrow'
                or str(captured['native']) not in names
            ):
                raise ValueError('Unregistered/mismatched native oracle evidence.')
            raw = _oracle_native(evidence_root, str(captured['native']), case_id)
            paths = _object(case_samples[0]['files'])
            derived = oracle(
                raw,
                case,
                evidence_root / str(paths['rallies.arrow']),
                evidence_root / str(paths['rally_cells.arrow']),
            )
            if any(captured[key] != value for key, value in derived.items()):
                failures.append(f'{case_id}: independent native oracle evidence differs.')
            for key, value in _object(derived['authentic_features']).items():
                features[key] += _integer(value)
            if case_id.startswith('positive_') and _integer(derived['event_count']) <= 0:
                failures.append(f'{case_id}: no authentic positive event.')
            for sample in case_samples[1:]:
                paths = _object(sample['files'])
                warm = oracle(
                    raw,
                    case,
                    evidence_root / str(paths['rallies.arrow']),
                    evidence_root / str(paths['rally_cells.arrow']),
                )
                if warm['content_hashes'] != derived['content_hashes']:
                    failures.append(
                        f'{case_id}: immutable membership content changed between rounds.'
                    )
        if any(value <= 0 for value in features.values()):
            failures.append(
                'Authentic ties/partiality/endpoint-overlap/price-excursion proof is absent.'
            )
        for sample in proofs:
            name = str(sample['case_id'])
            summary = next(_rows(evidence_root / str(_object(sample['files'])['summary.arrow'])))
            if name == 'empty' and summary['rally_count'] != 0:
                failures.append('Genuine empty analysis did not produce an empty result.')
            diagnostic = {
                'left': 'left_censored_count',
                'right': 'right_censored_count',
                'unknown': 'unknown_context_count',
            }.get(name)
            if diagnostic is not None and _integer(summary[diagnostic]) <= 0:
                failures.append(f'Authentic {name} diagnostic proof is absent.')
        capacity_rows = _records(
            _object(json.loads((evidence_root / 'source-capacity-before.json').read_bytes()))
        )
        if not capacity_rows:
            failures.append('Authentic source-capacity/disk floor is missing.')
        after_capacity_rows = _records(
            _object(json.loads((evidence_root / 'source-capacity-after.json').read_bytes()))
        )
        measured: dict[str, int] = {}
        for row in (*capacity_rows, *after_capacity_rows):
            key = str(row['source_key'])
            measured[key] = max(measured.get(key, 0), _integer(row['working_set_bytes']))
        failures.extend(_validate_resources(evidence_root, measured))
        failures.extend(
            _validate_statements(
                _object(json.loads((evidence_root / 'statements.json').read_bytes())), result_ids
            )
        )
        logs = _records(_object(json.loads((evidence_root / 'container-logs.json').read_bytes())))
        discovery_ids = {
            str(row['message']).split('market state rally ', 1)[1].split()[0]
            for row in logs
            if 'market state rally ' in str(row['message']) and ' published ' in str(row['message'])
        }
        if not result_ids <= discovery_ids:
            failures.append('Authoritative publication/container logs are missing.')
        for row in logs:
            match = re.search(
                r'market state rally ([0-9a-f-]{36}) published total_ms=(\d+) rss_peak_bytes=(\d+)',
                str(row['message']),
            )
            if match and (
                int(match[2]) > LIMITS['server_seconds'] * 1000
                or int(match[3]) > LIMITS['worker_rss_bytes']
            ):
                failures.append('Server discovery wall/RSS threshold exceeded.')
        failures.extend(_validate_reuse(evidence_root))
        failures.extend(_validate_concurrency(evidence_root))
        failures.extend(_validate_ingestion(evidence_root))
        corrected = _object(json.loads((evidence_root / 'corrected-revision.json').read_bytes()))
        revision_rows = _records(_object(corrected['production_evidence']))
        if corrected['status'] != 'deferred' or not corrected['reason'] or len(revision_rows) != 1:
            failures.append('Approved corrected-revision deferral is missing/unjustified.')
        elif _integer(revision_rows[0]['multi_revision_partitions']) != 0:
            failures.append(
                'Corrected-revision evidence now exists; deferral must be reassessed explicitly.'
            )
        legacy = _object(json.loads((evidence_root / 'legacy.json').read_bytes()))
        if legacy['exit_code'] != 0 or not _object(legacy['fixture_sha256']):
            failures.append('Unchanged 989/4 legacy fixture compatibility failed/unproved.')
    except (OSError, ValueError, KeyError, TypeError, StopIteration) as error:
        failures.append(
            f'Incomplete/unverifiable acceptance evidence: {type(error).__name__}: {error}'
        )
    return sorted(set(failures))


def verify_report(path: Path) -> list[str]:
    report = _object(json.loads(path.read_bytes()))
    checksum = path.with_suffix(path.suffix + '.sha256')
    if not checksum.is_file() or checksum.read_text().strip() != _sha(path):
        return ['Immutable report checksum is missing/mismatched.']
    root = path.with_suffix('.evidence')
    manifest = _object(json.loads((root / 'manifest.json').read_bytes()))
    failures = validate_report(report, manifest, root)
    if report.get('status') != 'PASS':
        failures.append('Recorded acceptance did not pass.')
    return failures


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    acceptance_parser = commands.add_parser('acceptance')
    acceptance_parser.add_argument('--url', default='http://127.0.0.1:8486')
    acceptance_parser.add_argument('--report', type=Path, required=True)
    verify_parser = commands.add_parser('verify-report')
    verify_parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == 'acceptance':
        return acceptance(str(args.url), cast(Path, args.report))
    try:
        failures = verify_report(cast(Path, args.report))
    except (OSError, ValueError, KeyError, TypeError) as error:
        failures = [f'Cannot verify immutable report: {error}']
    for failure in failures:
        print(failure, file=sys.stderr)
    print('FAIL' if failures else 'PASS')
    return 1 if failures else 0


if __name__ == '__main__':
    raise SystemExit(main())
