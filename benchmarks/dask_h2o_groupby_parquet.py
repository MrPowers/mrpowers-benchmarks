import dask.dataframe as dd
import pandas as pd
import dask
from helpers import benchmark, get_results
from dask_h2o_groupby_queries import *
import sys
from dask.distributed import Client, wait, LocalCluster

print("dask version: %s" % dask.__version__)

parquet_path = sys.argv[1]

if __name__ == "__main__":
    cluster = LocalCluster(n_workers=3, memory_limit="2.4GB")
    client = Client(cluster)

    dask_parquet_benchmarks = {
        "duration": [],
        "task": [],
    }

    ddf1 = dd.read_parquet(
        parquet_path, columns=["id1", "v1"], engine="pyarrow"
    ).persist()
    wait(ddf1)
    benchmark(q1, df=ddf1, benchmarks=dask_parquet_benchmarks, name="q1")
    del ddf1

    ddf2 = dd.read_parquet(
        parquet_path,
        columns=["id1", "id2", "v1"],
        engine="pyarrow",
    ).persist()
    wait(ddf2)
    benchmark(q2, df=ddf2, benchmarks=dask_parquet_benchmarks, name="q2")
    del ddf2

    ddf3 = dd.read_parquet(
        parquet_path,
        columns=["id3", "v1", "v3"],
        engine="pyarrow",
    ).persist()
    wait(ddf3)
    benchmark(q3, df=ddf3, benchmarks=dask_parquet_benchmarks, name="q3")
    del ddf3

    ddf4 = dd.read_parquet(
        parquet_path,
        columns=["id4", "v1", "v2", "v3"],
        engine="pyarrow",
    ).persist()
    wait(ddf4)
    benchmark(q4, df=ddf4, benchmarks=dask_parquet_benchmarks, name="q4")
    del ddf4

    ddf5 = dd.read_parquet(
        parquet_path,
        columns=["id6", "v1", "v2", "v3"],
        engine="pyarrow",
    ).persist()
    wait(ddf5)
    benchmark(q5, df=ddf5, benchmarks=dask_parquet_benchmarks, name="q5")
    del ddf5

    ddf7 = dd.read_parquet(
        parquet_path,
        columns=["id3", "v1", "v2"],
        engine="pyarrow",
    ).persist()
    wait(ddf7)
    benchmark(q7, df=ddf7, benchmarks=dask_parquet_benchmarks, name="q7")
    del ddf7

    ddf8 = dd.read_parquet(
        parquet_path, columns=["id6", "v1", "v2", "v3"], engine="pyarrow"
    ).persist()
    wait(ddf8)
    benchmark(q8, df=ddf8, benchmarks=dask_parquet_benchmarks, name="q8")
    del ddf8

    ddf9 = dd.read_parquet(
        parquet_path,
        columns=["id2", "id4", "v1", "v2"],
        engine="pyarrow",
    ).persist()
    wait(ddf9)
    benchmark(q9, df=ddf9, benchmarks=dask_parquet_benchmarks, name="q9")
    del ddf9

    dask_res_parquet_temp = get_results(dask_parquet_benchmarks).set_index("task")

    print(dask_res_parquet_temp)
