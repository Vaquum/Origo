import ast,hashlib,json,time
from pathlib import Path
from origo.assets.create_origo_database import get_clickhouse_settings,make_clickhouse_client
from origo.query import market_state
from origo.sources.binance_spot_trades import BINANCE_SPOT_TRADES_SPEC
from origo.sources.storage import SourceStore
baseline=Path(market_state.__file__).read_bytes()
assert hashlib.sha256(baseline).hexdigest()=='ef35fe88d1fedc7f324802dadf907df5163f6eddac0e012ca765002ad635509d'
candidate=Path('/tmp/candles-final-market-state.py').read_bytes()
assert hashlib.sha256(candidate).hexdigest()=='0db5b29259c640decaf463bf4e201c5dc423e9706797598ee3ddae469151cc4f'
functions={'current':market_state.pin}
node=next(n for n in ast.parse(candidate).body if isinstance(n,ast.FunctionDef) and n.name=='pin')
exec(compile(ast.Module(body=[node],type_ignores=[]),'/tmp/candles-final-market-state.py','exec'),market_state.__dict__)
functions['candidate']=market_state.pin
config=get_clickhouse_settings();client=make_clickhouse_client(config);store=SourceStore(client,config.database,BINANCE_SPOT_TRADES_SPEC)
execute=store.execute;observed={}
def traced(*args,**kwargs):
 start=time.perf_counter();rows=execute(*args,**kwargs)
 observed.update(manifest_ms=(time.perf_counter()-start)*1000,rows=len(rows),rows_digest=hashlib.sha256(repr(rows).encode()).hexdigest())
 return rows
store.execute=traced
settings={**market_state.QUERY_SETTINGS,'readonly':1,'max_threads':4,'max_execution_time':5,'max_memory_usage':536870912,'use_query_cache':0}
expected=None;rows_digest=None
print(json.dumps({'diagnostic_only':True,'candidate_commit':'3a7cbc5','candidate_module_sha256':hashlib.sha256(candidate).hexdigest()}),flush=True)
try:
 for pair in range(6):
  for kind in (['current','candidate'] if pair%2==0 else ['candidate','current']):
   start=time.perf_counter();answer=functions[kind](store,settings);elapsed=(time.perf_counter()-start)*1000
   if expected is None:expected=answer;rows_digest=observed['rows_digest']
   if answer!=expected or observed['rows_digest']!=rows_digest:raise RuntimeError('Source changed or full selected rows/pins differ')
   print(json.dumps({'diagnostic_only':True,'pair':pair,'kind':kind,'pin_ms':elapsed,**observed}),flush=True)
finally:client.disconnect()
