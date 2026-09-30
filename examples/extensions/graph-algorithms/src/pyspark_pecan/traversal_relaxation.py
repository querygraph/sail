"""One weighted expansion write, including evidence of dominated overflow."""
from pyspark.sql.connect import functions as F


_OVERFLOW = "__pecan_distance_overflow"


def weighted_relaxation(state, candidates):
    # The overflow reduction must see every candidate, not only the winning
    # distance: a finite existing label can dominate an overflowing relaxation.
    # Group both reductions together so they share the same expansion join.
    return state.unionByName(candidates).groupBy("id").agg(
        F.min(F.struct("distance", "hops", "parent")).alias("best"),
        F.bool_or(F.col("distance") == float("inf")).alias(_OVERFLOW),
    ).select("id", "best.*", _OVERFLOW)


def materialize_weighted_relaxation(run, frame):
    path, stored = run.materialize(frame)
    run.cancellation.check()
    overflow = stored.where(F.col(_OVERFLOW)).limit(1).count()
    run.cancellation.check()
    if overflow:
        raise OverflowError("shortest-path distance overflow")
    # This projection reads only the committed generation. No second write or
    # expansion is needed, and internal evidence never enters the public state.
    return path, stored.drop(_OVERFLOW)
