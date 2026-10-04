import hashlib,json,time,statistics
from origo.assets.create_origo_database import get_clickhouse_settings,make_clickhouse_client
client=make_clickhouse_client(get_clickhouse_settings())
settings={'readonly':1,'max_threads':4,'max_execution_time':5,'max_memory_usage':536870912,'use_query_cache':0}
params={'source':'binance_spot_trades'}
columns='partition_key, provisional, partition_start, partition_end, generation, revision, build_id, component_hashes'
queries={
'current':f'SELECT {columns} FROM origo.source_current_partitions WHERE source_key=%(source)s ORDER BY partition_start, provisional',
'single_pass':f'''WITH covered AS (
 SELECT a.*, maxIf(partition_end, NOT provisional) OVER (
  PARTITION BY source_key ORDER BY partition_start, provisional
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS canonical_end
 FROM origo.source_active_partitions a WHERE source_key=%(source)s
), eligible AS (
 SELECT * EXCEPT canonical_end FROM covered
 WHERE NOT provisional OR partition_start>=canonical_end
), ranked AS (
 SELECT e.*, anchor, max(partition_end) OVER (
  PARTITION BY e.source_key ORDER BY partition_start, partition_end
  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_end
 FROM eligible e INNER JOIN origo.source_anchor_log n ON e.source_key=n.source_key
), bounded AS (
 SELECT *, if(countIf(partition_start>greatest(prior_end,anchor)) OVER (PARTITION BY source_key)=0,
 max(partition_end) OVER (PARTITION BY source_key),
 minIf(partition_start,partition_start>greatest(prior_end,anchor)) OVER (PARTITION BY source_key)) AS frontier
 FROM ranked
)
SELECT {columns} FROM bounded
WHERE NOT provisional OR partition_end<=frontier ORDER BY partition_start, provisional'''}
def read(kind,pair):
 started=time.perf_counter();rows=client.execute(queries[kind],params,settings={**settings,'log_comment':f'candles-manifest-{kind}-{pair}'})
 ms=(time.perf_counter()-started)*1000
 digest=hashlib.sha256(repr(rows).encode()).hexdigest()
 print(json.dumps({'diagnostic_only':True,'kind':kind,'pair':pair,'ms':ms,'rows':len(rows),'digest':digest}),flush=True)
 return rows,ms
try:
 baseline,_=read('current','initial');candidate,_=read('single_pass','initial')
 if baseline!=candidate:raise RuntimeError('Query results differ; do not benchmark as an equivalent optimization')
 for pair in range(6):
  for kind in (['current','single_pass'] if pair%2==0 else ['single_pass','current']):
   rows,ms=read(kind,pair)
   if rows!=baseline:raise RuntimeError('Accepted source state changed; diagnostic timing comparison invalid')
finally:client.disconnect()
