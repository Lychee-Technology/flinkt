import re, sys
sys.argv=['x']
exec(open('analyze.py').read().split('def main')[0])
for l in LINES:
    rows=[r for r in load(l) if r['kind']=='METHOD' and r['cls'] in FACADE and 'static' not in r['mods'] and 'protected' not in r['mods'] and r['decl']!='Object']
    uniq={}
    for r in rows:
        uniq.setdefault((r['cls'], r['name']+r['params']), r)
    stream_nonfinal=set(); builder=set(); sink=set(); intro=set(); finals=set(); total=set()
    for (cls,sig),r in uniq.items():
        kind,base,i=classify(r,cls)
        key=(cls,sig)
        total.add(key)
        if 'final' in r['mods']: finals.add(key); continue
        if kind=='stream': stream_nonfinal.add(key)
        if kind=='builder': builder.add(key)
        if kind=='sink': sink.add(key)
        if i: intro.add(key)
    print(l, 'reachable (class,method) pairs:', len(total), '| stream-returning non-final (subtype must override covariantly per class):', len(stream_nonfinal), '| builder-returning (exit unless wrapped):', len(builder), '| sink-returning:', len(sink), '| type-introducing (override cannot be reified):', len(intro), '| final:', len(finals))
    # KeyedStream methods overriding DataStream ones
    ks=[r for r in load(l) if r['kind']=='METHOD' and r['cls']=='KeyedStream' and r['decl']=='KeyedStream']
    ds={r['name']+r['params'] for r in load(l) if r['kind']=='METHOD' and r['cls']=='DataStream' and r['decl']=='DataStream'}
    print('   KeyedStream overrides of DataStream methods:', sorted(r['name']+re.sub(r'org[.]apache[.]flink[.][a-z.]*[.]','',r['params'])+(' [protected]' if 'protected' in r['mods'] else '') for r in load(l) if r['kind']=='METHOD' and r['cls']=='KeyedStream' and r['decl']=='KeyedStream' and (r['name']+r['params']) in {x['name']+x['params'] for x in load(l) if x['kind']=='METHOD' and x['cls']=='DataStream'}))
    print('   SOSO overrides of DataStream methods:', sorted(r['name'] for r in load(l) if r['kind']=='METHOD' and r['cls']=='SingleOutputStreamOperator' and r['decl']=='SingleOutputStreamOperator' and (r['name']+r['params']) in {x['name']+x['params'] for x in load(l) if x['kind']=='METHOD' and x['cls']=='DataStream'}))
    print('   DataStreamSource overrides:', sorted(r['name']+re.sub(r'org[.]apache[.]flink[.][a-z.]*[.]','',r['params']) for r in load(l) if r['kind']=='METHOD' and r['cls']=='DataStreamSource' and r['decl']=='DataStreamSource'))
