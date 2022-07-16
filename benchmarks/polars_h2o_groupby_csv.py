import polars as pl
from helpers import benchmark, get_results
import sys
import glob
import pandas as pd
from polars_h2o_groupby_queries import *

path = sys.argv[1]


with pl.StringCache():
    df = pl.read_csv(
        path,
        dtype={
            "id1": pl.Utf8,
            "id2": pl.Utf8,
            "id3": pl.Utf8,
            "id4": pl.Int32,
            "id5": pl.Int32,
            "id6": pl.Int32,
            "v1": pl.Int32,
            "v2": pl.Int32,
            "v3": pl.Float64,
        },
        low_memory=True,
    ).with_columns(
        [
            pl.col("id1").cast(pl.Categorical),
            pl.col("id2").cast(pl.Categorical),
            pl.col("id3").cast(pl.Categorical),
        ]
    )

# df_csv_lazy = df_csv.lazy()

# df_csv_lazy = pl.scan_csv(path)

polars_single_csv_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=df, benchmarks=polars_single_csv_benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=polars_single_csv_benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=polars_single_csv_benchmarks, name="q3")
benchmark(q4, df=df, benchmarks=polars_single_csv_benchmarks, name="q4")
benchmark(q5, df=df, benchmarks=polars_single_csv_benchmarks, name="q5")
benchmark(q6, df=df, benchmarks=polars_single_csv_benchmarks, name="q6")
benchmark(q7, df=df, benchmarks=polars_single_csv_benchmarks, name="q7")
benchmark(q8, df=df, benchmarks=polars_single_csv_benchmarks, name="q8")
benchmark(q9, df=df, benchmarks=polars_single_csv_benchmarks, name="q9")
# benchmark(q10, df=df, benchmarks=polars_single_csv_benchmarks, name="q10")

polars_res_single_csv_temp = get_results(polars_single_csv_benchmarks).set_index("task")

print(polars_res_single_csv_temp)
