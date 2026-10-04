import ast,hashlib,json,logging,os,shutil,signal,sys,threading,uuid
from functools import lru_cache
from pathlib import Path
from origo.query import market_state
from origo.query.market_state_results import RESULT_ROOT,ResultStore
from origo.workers.market_state_api import ApiServer,MarketStateApi,Reporter,DEFAULT_LOCK_ROOT,DEFAULT_WEBSERVER_URL
baseline=Path(market_state.__file__).read_bytes()
assert hashlib.sha256(baseline).hexdigest()=='0db5b29259c640decaf463bf4e201c5dc423e9706797598ee3ddae469151cc4f'
candidate=Path('/tmp/candles-record-decode-market-state.py').read_bytes()
assert hashlib.sha256(candidate).hexdigest()=='76f2c1a092c08cb5784b7360d663952e1f82914c86779607150938095c19069e'
market_state._cached_record=lru_cache(maxsize=8192)(market_state._record)
node=next(node for node in ast.parse(candidate).body if isinstance(node,ast.FunctionDef) and node.name=='pin')
exec(compile(ast.Module(body=[node],type_ignores=[]),'/tmp/candles-record-decode-market-state.py','exec'),market_state.__dict__)
root=RESULT_ROOT/('.record-decode-profile-'+str(uuid.uuid4()))
logging.basicConfig(filename='/tmp/candles-record-decode-api.log',level=logging.INFO,format='%(asctime)s %(levelname)s %(name)s %(message)s')
store=ResultStore(root)
api=MarketStateApi(store,Reporter(os.environ.get('DAGSTER_WEBSERVER_URL',DEFAULT_WEBSERVER_URL)),Path(os.environ.get('ORIGO_SOURCE_LOCK_DIR',DEFAULT_LOCK_ROOT)),port=8489)
server=ApiServer((sys.argv[1],8489),api)
thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
stop=threading.Event()
for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda signum,frame:stop.set())
print(json.dumps({'pid':os.getpid(),'candidate_sha256':hashlib.sha256(candidate).hexdigest(),'root':str(root),'bind':server.server_address}),flush=True)
try:stop.wait(900)
finally:
 server.shutdown();server.server_close();thread.join()
 assert root.parent==RESULT_ROOT and root.name.startswith('.record-decode-profile-')
 shutil.rmtree(root)
 print('diagnostic service stopped; owned results removed',flush=True)
