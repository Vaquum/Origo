import sys,json,time,math,os,importlib.util,statistics,concurrent.futures
from pathlib import Path
import numpy as np
sys.path.insert(0,'/app/tools')
import cube_bridge as old
spec=importlib.util.spec_from_file_location('candidate','/tmp/candle_bridge.py');new=importlib.util.module_from_spec(spec);sys.modules['candidate']=new;spec.loader.exec_module(new)
new.CUBE_URL='http://172.18.0.16:8489'
config=json.loads(Path('/tmp/candle-navigation-v2.json').read_text())['realHost']
response,_,_=new.cube_query(t1=new.edge(3214000),t2=new.edge(3214001),tR=56.25,pR=125,p1=None,p2=None)
cut=math.floor(new.base_units(response['data_cutoff']));canon=new.base_units(response['canonical_through']);rng=np.random.default_rng(20260930)
print(json.dumps({'protocol':'navigation.v2 realHost unchanged','baseline_sha':'a42face90982ebd2b272d54ff989b1f13bde3202','candidate_origo_base_sha':'51f6be1','candidate_origo_module_sha256':'9e3e7755492dd2659860e6c9de568c350cb4e1db1bed366342293834f1d1b36d','isolated_api':True,'candidate_bridge_sha256':__import__('hashlib').sha256(Path('/tmp/candle_bridge.py').read_bytes()).hexdigest(),'baseline_bridge_sha256':__import__('hashlib').sha256(Path('/app/tools/cube_bridge.py').read_bytes()).hexdigest(),'config_sha256':__import__('hashlib').sha256(Path('/tmp/candle-navigation-v2.json').read_bytes()).hexdigest(),'utc_started':__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()}),flush=True)
records=[]
def measured(fn):
 t=time.perf_counter();v=fn();return (v['latency_ms'] if isinstance(v,dict) and 'latency_ms' in v else (time.perf_counter()-t)*1000),v
def trial(n,which):
 step=2**n;a=max(0,(math.floor(canon)//step-3)*step);b=a+step
 response,raw,pins=new.bar_read(n,a,b)
 expected_required_pins=old.read(4,0,a,a+16)[3]
 if new.read(4,0,a,a+16)[3]!=expected_required_pins:raise RuntimeError('Required-read pinned identities differ before sampling')
 held={'cutoff':cut,'pins':pins,'state':{'cutoff':response['data_cutoff'],'canonical_through':response['canonical_through']}}
 modules=[old,new];explorers=[m.Explorer(Path('/app/index.html')) for m in modules]
 for e in explorers:e.holding=lambda token:held
 def endpoint(i):
  explorers[i].bar_answers.clear();return explorers[i].bars(n,a,b,'fixed')
 def required(i):
  if i==0:
   t=time.perf_counter();v=old.read(4,0,a,a+16);elapsed=(time.perf_counter()-t)*1000
   if v[3]!=expected_required_pins:raise RuntimeError('Baseline source pin changed during sampling')
   return {'latency_ms':elapsed}
  with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
   other=pool.submit(new.bar_read,n,a,b)
   t=time.perf_counter();v=new.read(4,0,a,a+16);elapsed=(time.perf_counter()-t)*1000;bar=other.result()
   if v[3]!=expected_required_pins or bar[2]!=pins:raise RuntimeError('Candidate source pin changed during sampling')
   return {'latency_ms':elapsed}
 fn=endpoint if which=='bars' else required
 # Warm identical cached cube queries before protocol samples.
 fn(0);fn(1)
 def pair(phase,k,aa=False):
  order=[0,1] if (phase=='aa' and k%2==0) or (phase!='aa' and rng.random()<.5) else [1,0]
  result={}
  for i in order:
   ms,_=measured(lambda:fn(0 if aa else i));result[i]=ms
  rec={'case':which+'-'+str(n),'phase':phase,'pair':k,'A_ms':result[0],'B_ms':result[1],'difference_ms':result[1]-result[0],'order':order};records.append(rec);print(json.dumps(rec),flush=True)
  return result[1]-result[0]
 aa=[pair('aa',k,True) for k in range(config['aaPairs'])]
 screening=[pair('screening',k) for k in range(config['screeningPairs'])]
 base=statistics.median(r['A_ms'] for r in records if r['case']==which+'-'+str(n) and r['phase']=='screening')
 floor=max(config['absoluteRegressionFloorMs'],base*config['relativeRegressionFloor'],float(np.quantile(aa,.95)))
 verdict='no-regression-detected-at-this-resolution';interval=None
 if statistics.median(screening)>floor:
  diffs=np.array([pair('confirmation',k) for k in range(config['confirmationPairs'])]);means=np.mean(rng.choice(diffs,(10000,len(diffs)),replace=True),axis=1);interval=[float(np.quantile(means,.0005)),float(np.quantile(means,.9995))]
  verdict='blocking' if interval[0]>floor else 'pass' if interval[1]<=floor else 'inconclusive'
 return {'case':which+'-'+str(n),'baseline_median_ms':base,'floor_ms':floor,'screening_median_difference_ms':statistics.median(screening),'interval_ms':interval,'verdict':verdict}
results=[]
for n in config['supportedLevels']:
 for which in ['bars','required-read']:
  result=trial(n,which);results.append(result);print(json.dumps({'result':result}),flush=True)
print(json.dumps({'cutoff':response['data_cutoff'],'canonical_through':response['canonical_through'],'results':results,'pass':all(x['verdict'] in ['pass','no-regression-detected-at-this-resolution'] for x in results)}),flush=True)
