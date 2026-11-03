"""Run Pecan's reference or optimized PageRank and WCC through Spark Connect."""

import argparse
import os

os.environ.setdefault("SPARK_CONNECT_MODE_ENABLED", "1")

from pyspark.sql.connect.session import SparkSession
from pyspark_pecan import GraphAlgorithms


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", default="sc://localhost:50051")
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--pagerank-method", choices=("power", "delta"), default="power")
    parser.add_argument("--wcc-method", choices=("min_label", "randomized"), default="min_label")
    parser.add_argument("--iterations", type=int,
                        help="PageRank step/push limit (default: power 10, delta 1000)")
    parser.add_argument("--tolerance", type=float,
                        help="power: optional L1 step threshold; delta: global residual threshold (default 1e-8)")
    parser.add_argument("--wcc-iterations", type=int, default=100,
                        help="WCC propagation/contraction round limit (default 100)")
    parser.add_argument("--seed", type=int, default=42,
                        help="randomized WCC unsigned 64-bit seed (default 42)")
    args = parser.parse_args()
    iterations = args.iterations if args.iterations is not None else (1000 if args.pagerank_method == "delta" else 10)
    tolerance = args.tolerance
    if tolerance is None and args.pagerank_method == "delta":
        tolerance = 1e-8
    rank_options = dict(method=args.pagerank_method, max_iterations=iterations,
                        partitions=args.partitions)
    if tolerance is not None:
        rank_options["tolerance"] = tolerance
    spark = SparkSession.builder.remote(args.remote).create()
    try:
        vertices = spark.createDataFrame([(0,), (1,), (2,), (3,), (9,)], "id long")
        edges = spark.createDataFrame([(0, 1), (1, 2), (2, 0), (2, 3)], "src long, dst long")
        graph = GraphAlgorithms(spark)
        with graph.pagerank(vertices, edges, **rank_options) as result:
            if args.pagerank_method == "delta":
                print(f"delta PageRank: pushes={result.iterations}, residual={result.residual:.3e}, "
                      f"stationary L1 error bound={result.error_bound:.3e}")
            result.frame.orderBy("id").show()
        with graph.wcc(vertices, edges, method=args.wcc_method, max_iterations=args.wcc_iterations,
                       seed=args.seed, partitions=args.partitions) as result:
            if args.wcc_method == "randomized":
                print(f"randomized WCC: seed={result.seed}, contractions={result.iterations}")
            result.frame.orderBy("id").show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
