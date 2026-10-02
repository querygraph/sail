"""Traversal execution adapters and distributed full-vector validation."""
from pathlib import Path
import sys
import time
from pyspark.sql.connect import functions as F
from pyspark_pecan import GraphAlgorithms
from sail_nutmeg.client import Nutmeg


from traversal_methods import method


def stage_order(args):
    """The native staging order to send: None for the server default (canonical)."""
    order=getattr(args,'stage_order','canonical')
    return None if order=='canonical' else order


def execute(spark,args,receipt,sampler):
    vertices=spark.read.parquet((args.dataset/'vertices.parquet').as_uri())
    edges=spark.read.parquet((args.dataset/'edges.parquet').as_uri())
    selected=method(args.engine,args.algorithm,args.variant)
    nm=handle=None
    output=args.output/'result'
    events=[]
    sampler.mark('execute')
    started=time.perf_counter()
    try:
        if args.engine == 'nutmeg-native':
            nm=Nutmeg(spark)
            kind='long' if getattr(args,'native_ids','string')=='int64' else 'string'
            receipt['native_ids']=getattr(args,'native_ids','string')
            nodes=vertices.select(F.col('id').cast(kind).alias('node_id'))
            links=edges.select(F.col('src').cast(kind).alias('source'),
                               F.col('dst').cast(kind).alias('target'),'weight')
            mapping={'ids':'int64'} if kind=='long' else None
            receipt['stage_receipt']=nm.stage('benchmark',nodes,links,node_mapping=mapping,edge_mapping=mapping,
                                              order=stage_order(args)).asDict()
            receipt['stage_seconds']=time.perf_counter()-started
            options=dict(source=str(args.source), concurrency=args.threads,
                         orientation='outgoing' if args.directed else 'undirected')
            if args.algorithm == 'sssp':
                options['weightProperty']='weight'
            if selected in ('bfsDirection','ssspDeltaStar'):
                options['maxIterations']=args.max_iterations
            if selected=='ssspDeltaStar':
                options['delta']=args.delta
            frame=nm.run('benchmark',selected,**options)
            exported=frame.select(F.col('nodeId').cast('long').alias('id'),F.col('distance').cast('double'))
            if selected=='bfsDirection':
                exported=frame.select(F.col('nodeId').cast('long').alias('id'),
                    F.col('distance').cast('double'),F.col('parentId').cast('long').alias('parent'),
                    F.col('distance').cast('long').alias('hops'))
                receipt['parent_output']='native BFS rooted parent tree'
            else:
                receipt['parent_output']='not exposed by native distance kernel'
        elif args.engine == 'argentea':
            # Native partitions on the Sail workers: one job unrolls init, the rounds and the
            # result stage; graph rows never collect in this client. The clients live beside the
            # Argentea sources, not in an installed package.
            sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'argentea/python'))
            cap=args.argentea_max_rounds
            partitions=min(args.partitions,64)
            def observe(event):
                events.append(dict({k:v for k,v in event.items() if k not in ('frame','plan_bytes','view_registrations','request')},
                                   plan_bytes=len(event['plan_bytes']) if 'plan_bytes' in event else None,
                                   elapsed_seconds=time.perf_counter()-started))
            if args.algorithm=='bfs':
                from argentea_bfs_client import ArgenteaBfs
                handle=ArgenteaBfs(spark,observer=observe).bfs(vertices,edges,source=args.source,method=selected,
                    directed=args.directed,max_levels=cap,partitions=partitions,max_phase_budget=2*cap+4)
            else:
                from argentea_sssp_client import ArgenteaSssp
                handle=ArgenteaSssp(spark,observer=observe).sssp(vertices,edges,source=args.source,method=selected,
                    directed=args.directed,max_rounds=cap,partitions=partitions,delta=args.delta,max_phase_budget=2*cap+4)
            receipt.update(algorithm_ready_seconds=time.perf_counter()-started,
                           algorithm_iterations=handle.iterations,algorithm_converged=handle.converged,
                           argentea=dict(reached=handle.reached,phase=handle.phase,native_phase_count=handle.native_phase_count,
                                         partitions=partitions,rounds_cap=cap))
            exported=handle.frame
            receipt['parent_output']='rooted parent tree and hop count (owned rows)'
        else:
            if args.engine == 'nutmeg-datafusion':
                tables=Nutmeg(spark).tables(vertices,edges,node_id='id',source='src',target='dst')
                vertices=tables.nodes.select(F.col('node_id').alias('id'))
                edges=tables.edges.select(F.col('source').alias('src'),F.col('target').alias('dst'),'weight')
            graph=GraphAlgorithms(spark,observer=lambda event:events.append(
                dict({k:v for k,v in event.as_dict().items() if k!='run_path'},elapsed_seconds=time.perf_counter()-started)),
                record_plans=getattr(args,'record_plans',False),snapshot_inputs=getattr(args,'snapshot_inputs',True))
            receipt['pecan_input_policy'] = 'assume_valid_finite_path_sums'
            options=dict(source=args.source,method=selected,directed=args.directed,
                         partitions=args.partitions,max_iterations=args.max_iterations)
            if args.algorithm=='sssp':
                options['delta']=args.delta
            handle=getattr(graph,args.algorithm)(vertices,edges,**options)
            receipt.update(algorithm_ready_seconds=time.perf_counter()-started,
                           algorithm_iterations=handle.iterations,algorithm_converged=handle.converged)
            exported=handle.frame
            receipt['parent_output']='rooted parent tree and hop count'
        receipt['kernel']=selected
        exported.write.mode('error').parquet(output.as_uri())
        receipt['end_to_end_seconds']=time.perf_counter()-started
        return output,handle,nm
    except BaseException:
        receipt['elapsed_until_error_seconds']=time.perf_counter()-started
        # Ownership is retained by the server session even if no handle returns.
        raise
    finally:
        receipt['iteration_events']=events


def validate(spark,output,dataset,algorithm,expected_rows,source,directed,native,
             *, policy="reference", certificate_max_rounds=10000, partitions=4):
    actual=spark.read.parquet(output.as_uri())
    if policy == 'certificate':
        from traversal_certificate import certify
        vertices=spark.read.parquet((dataset/'vertices.parquet').as_uri())
        edges=spark.read.parquet((dataset/'edges.parquet').as_uri())
        result=certify(spark,actual,vertices,edges,source=source,weighted=algorithm=='sssp',
                       directed=directed,partitions=partitions,max_rounds=certificate_max_rounds,
                       tolerance=1e-12 if algorithm=='sssp' else 0.)
        result['parent_tree_checked']=validate_parents(
            spark,actual,dataset,algorithm,expected_rows,source,directed,native)
        return result
    if policy != 'reference':
        raise ValueError('unknown traversal validation policy')
    expected=spark.read.parquet((dataset/'reference.parquet').as_uri()).select(
        'id',F.col(algorithm).alias('expected'))
    stats=actual.agg(F.count('*').alias('rows'),F.countDistinct('id').alias('unique')).first().asDict()
    assert stats==dict(rows=expected_rows,unique=expected_rows),stats
    assert not actual.join(expected,'id','left_anti').limit(1).count()
    joined=actual.join(expected,'id')
    invalid=joined.where(
        F.col('distance').isNull()!=F.col('expected').isNull())
    assert not invalid.limit(1).count(),'reachability differs'
    finite=joined.where(F.col('expected').isNotNull())
    assert not finite.where(F.isnan('distance') | (F.abs('distance')==float('inf')) |
        (F.col('distance')<0) |
        (F.abs(F.col('distance')-F.col('expected')) > F.lit(1e-12)*(1+F.abs('expected')))
    ).limit(1).count(),'distance differs from independent reference'
    parents_present=validate_parents(spark,actual,dataset,algorithm,expected_rows,source,directed,native)
    return dict(**stats,parent_tree_checked=parents_present,reference='independent BFS/heap-Dijkstra')


def validate_parents(spark,actual,dataset,algorithm,expected_rows,source,directed,native):
    parents_present='parent' in actual.columns and 'hops' in actual.columns
    if not native:
        assert parents_present, 'portable traversal must return parents and hops'
    if parents_present:
        assert not actual.where(F.col('distance').isNull() &
            (F.col('parent').isNotNull() | F.col('hops').isNotNull())).limit(1).count()
        root=actual.where(F.col('id')==source).first()
        assert root is not None and root.parent==source and root.hops==0 and root.distance==0
        reached=actual.where(F.col('distance').isNotNull() & (F.col('id')!=source))
        parents=actual.select(F.col('id').alias('parent'),F.col('distance').alias('parent_distance'),
                              F.col('hops').alias('parent_hops'))
        linked=reached.join(parents,'parent','left')
        assert not linked.where(F.col('parent_distance').isNull() | F.col('hops').isNull() |
            (F.col('hops')!=F.col('parent_hops')+1) | (F.col('hops')<1) |
            (F.col('hops')>=expected_rows)).limit(1).count(),'invalid rooted parent chain'
        edges=spark.read.parquet((dataset/'edges.parquet').as_uri())
        if not directed:
            edges=edges.unionByName(edges.select(F.col('dst').alias('src'),F.col('src').alias('dst'),'weight'))
        if algorithm=='bfs':
            edges=edges.withColumn('weight',F.lit(1.))
        candidates=linked.join(edges,(linked.parent==edges.src)&(linked.id==edges.dst)).where(
            F.abs(F.col('distance')-F.col('parent_distance')-F.col('weight')) <=
            F.lit(1e-12)*(1+F.abs('distance'))).select('id').distinct()
        assert not reached.join(candidates,'id','left_anti').limit(1).count(),'invalid parent edge'
    return parents_present
