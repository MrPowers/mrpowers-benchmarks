import dask.dataframe as dd
import pandas as pd
import dask
from helpers import benchmark, get_results
import sys

print("dask version: %s" % dask.__version__)

dataset = sys.argv[1]

dask_parquet_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

parquet_path = f"./data/mrpowers-h2o/groupby-{dataset}/parquet"

ddf1 = dd.read_parquet(parquet_path, columns=["id4"], engine="pyarrow")
benchmark(q1, df=ddf1, benchmarks=dask_parquet_benchmarks, name="q1")

ddf2 = dd.read_parquet(
    parquet_path,
    columns=["id4"],
    engine="pyarrow",
)
benchmark(q2, df=ddf2, benchmarks=dask_parquet_benchmarks, name="q2")

ddf3 = dd.read_parquet(
    parquet_path,
    columns=["v3"],
    engine="pyarrow",
)
benchmark(q3, df=ddf3, benchmarks=dask_parquet_benchmarks, name="q3")

ddf4 = dd.read_parquet(
    parquet_path,
    columns=["id5", "v3"],
    engine="pyarrow",
)
benchmark(q4, df=ddf4, benchmarks=dask_parquet_benchmarks, name="q4")

ddf5 = dd.read_parquet(
    parquet_path,
    columns=["id2", "id5"],
    engine="pyarrow",
)
benchmark(q5, df=ddf5, benchmarks=dask_parquet_benchmarks, name="q5")

dask_res_parquet_temp = get_results(dask_parquet_benchmarks).set_index("task")

print(dask_res_parquet_temp)
