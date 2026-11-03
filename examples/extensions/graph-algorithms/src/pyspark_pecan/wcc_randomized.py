"""Seeded randomized contraction with stable IDs and reverse label expansion.

The closed-neighborhood contraction follows Bögeholz, Brand and Todor,
In-database connected component analysis (ICDE 2020). Random GF64 priorities
choose representatives, while representative identities remain original BIGINT
IDs. This avoids mixing hashes from different rounds or with isolated IDs.
"""
from pyspark.sql.connect import functions as F

MASK = (1 << 64) - 1


def signed(value):
    return value if value < (1 << 63) else value - (1 << 64)


class SplitMix64:
    """Version-independent coefficient stream shared with the native kernel."""
    def __init__(self, seed):
        if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed <= MASK:
            raise ValueError('seed must be an unsigned 64-bit integer')
        self.state = seed

    def next(self):
        self.state = (self.state + 0x9E3779B97F4A7C15) & MASK
        value = self.state
        value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK
        value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK
        return value ^ (value >> 31)

    def coefficients(self):
        a = self.next()
        while a == 0:
            a = self.next()
        return signed(a), signed(self.next())


def _canonical_edges(edges):
    return edges.where(F.col('src') != F.col('dst')).select(
        F.least('src', 'dst').alias('src'), F.greatest('src', 'dst').alias('dst')).distinct()


def execute(graph, vertices, edges, *, max_iterations, partitions, cancellation, seed, fused=False):
    from .algorithms import ConvergenceError
    random = SplitMix64(seed)
    if 'axpb' not in graph.utils.capabilities:
        raise ValueError('randomized WCC requires the axpb capability')
    algorithm = 'wcc-randomized-fused-contraction' if fused else 'wcc-randomized-contraction'

    def contract(run, vertices, edges, size):
        if fused:
            # _run already snapshotted these edges. Keep duplicate/oriented
            # rows for the first round; canonicalize only after contraction.
            edge_path, current = None, edges.where(F.col('src') != F.col('dst'))
        else:
            edge_path, current = run.materialize(_canonical_edges(edges))
        run.cancellation.check()
        remaining = current.count()
        history = []
        steps = []
        while remaining:
            if len(history) >= max_iterations:
                raise ConvergenceError(f'WCC contraction did not finish in {max_iterations} iterations')
            step = len(history) + 1
            run.cancellation.check()
            graph._observe(run, algorithm, step, 'iteration_start')
            a, b = random.coefficients()
            priority_path = None
            if fused:
                from .wcc_fused import representatives
                rep_path, reps = run.materialize(representatives(current, a, b))
                run.cancellation.check()
                active_count = reps.count()
            else:
                active = current.select(F.col('src').alias('id')).unionByName(
                    current.select(F.col('dst').alias('id'))).distinct()
                priority_path, priorities = run.materialize(active.select('id', F.call_function(
                    'gf_axpb', F.lit(a).cast('long'), F.col('id'), F.lit(b).cast('long')).alias('priority')))
                run.cancellation.check()
                active_count = priorities.count()
                neighbors = current.select(F.col('src').alias('vertex'), F.col('dst').alias('neighbor')).unionByName(
                    current.select(F.col('dst').alias('vertex'), F.col('src').alias('neighbor'))).unionByName(
                    priorities.select(F.col('id').alias('vertex'), F.col('id').alias('neighbor')))
                ranked = neighbors.join(priorities, neighbors.neighbor == priorities.id).select('vertex', 'priority')
                minima = ranked.groupBy('vertex').agg(F.min('priority').alias('chosen_priority'))
                original_representatives = minima.join(priorities, minima.chosen_priority == priorities.priority).select(
                    minima.vertex.alias('id'), priorities.id.alias('representative'))
                rep_path, reps = run.materialize(original_representatives, expected_rows=active_count)
            history.append((rep_path, reps))
            # Materialize only after both endpoint joins, then release the old
            # edge and priority generations. Retain reps for the reverse pass.
            src_map = reps.select(F.col('id').alias('old_src'), F.col('representative').alias('new_src'))
            dst_map = reps.select(F.col('id').alias('old_dst'), F.col('representative').alias('new_dst'))
            relabeled = current.join(src_map, current.src == src_map.old_src).join(
                dst_map, current.dst == dst_map.old_dst).select(
                    F.col('new_src').alias('src'), F.col('new_dst').alias('dst'))
            next_path, next_edges = run.materialize(_canonical_edges(relabeled))
            run.cancellation.check()
            next_count = next_edges.count()
            if edge_path is not None:
                run.remove(edge_path)
            if priority_path is not None:
                run.remove(priority_path)
            edge_path, current = next_path, next_edges
            metrics = dict(active_vertices=active_count, edges_before=remaining,
                           edges_after=next_count, coefficient_a=a, coefficient_b=b)
            steps.append(metrics)
            graph._observe(run, algorithm, step, 'iteration_end', **metrics)
            remaining = next_count
        if edge_path is not None:
            run.remove(edge_path)
        frontier_path = None
        if history:
            last_path, last = history[-1]
            frontier_path, frontier = run.materialize(last.select('id', F.col('representative').alias('component')))
            run.remove(last_path)
            for old_path, older in reversed(history[:-1]):
                run.cancellation.check()
                later = frontier.select(F.col('id').alias('later_id'), F.col('component').alias('later_component'))
                expanded = older.join(later, older.representative == later.later_id, 'left').select(
                    older.id.alias('id'), F.coalesce('later_component', 'representative').alias('component'))
                expanded_path, expanded = run.materialize(expanded)
                run.remove(frontier_path)
                run.remove(old_path)
                frontier_path, frontier = expanded_path, expanded
            raw = vertices.join(frontier, 'id', 'left').select('id', F.coalesce('component', 'id').alias('component'))
        else:
            raw = vertices.select('id', F.col('id').alias('component'))
        raw_path, raw = run.materialize(raw, expected_rows=size)
        minima = raw.groupBy('component').agg(F.min('id').alias('minimum_id'))
        normalized = raw.join(minima, 'component').select('id', F.col('minimum_id').alias('component'))
        path, result = run.materialize(normalized, expected_rows=size)
        handle = run.finish(path, result, algorithm=algorithm,
                            iterations=len(history), converged=True)
        handle.method = 'randomized_fused' if fused else 'randomized'
        handle.seed = seed
        handle.contractions = steps
        return handle

    return graph._run(vertices, edges, partitions, cancellation, contract)
