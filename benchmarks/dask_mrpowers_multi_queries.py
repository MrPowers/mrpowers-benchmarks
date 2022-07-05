import dask.dataframe as dd
import pandas as pd
import dask
from helpers import benchmark, get_results
import sys

print("dask version: %s" % dask.__version__)

dataset = sys.argv[1]


def q1(ddf):
    return ddf.loc[ddf["id2"] == "id089"].groupby("id1").id4.sum().compute()


def q2(ddf):
    t = ddf.groupby("id3").agg({"id4": "sum", "v2": "mean"})
    return t.loc[(t["id4"] > 50) & (t["v2"] > 5.0)].compute()


# Parquet

dask_parquet_benchmarks = {
    "duration": [],
    "task": [],
}

parquet_path = f"./data/mrpowers-h2o/groupby-{dataset}/parquet"

ddf1 = dd.read_parquet(parquet_path, columns=["id1", "id2", "id4"], engine="pyarrow")
benchmark(q1, df=ddf1, benchmarks=dask_parquet_benchmarks, name="q1")

ddf2 = dd.read_parquet(parquet_path, columns=["id3", "id4", "v2"], engine="pyarrow")
benchmark(q2, df=ddf2, benchmarks=dask_parquet_benchmarks, name="q2")

dask_res_parquet_temp = get_results(dask_parquet_benchmarks).set_index("task")

print(dask_res_parquet_temp)

# CSV

dask_data = dd.read_csv(f"./data/mrpowers-h2o/groupby-{dataset}/csv/*.csv")

dask_csv_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=dask_data, benchmarks=dask_csv_benchmarks, name="q1")
benchmark(q2, df=dask_data, benchmarks=dask_csv_benchmarks, name="q2")

dask_res_csv_temp = get_results(dask_csv_benchmarks).set_index("task")

# Single CSV

dask_data = dd.read_csv(f"./data/mrpowers-h2o/groupby-{dataset}/single-csv/*.csv")

dask_single_csv_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=dask_data, benchmarks=dask_single_csv_benchmarks, name="q1")
benchmark(q2, df=dask_data, benchmarks=dask_single_csv_benchmarks, name="q2")

dask_res_single_csv_temp = get_results(dask_single_csv_benchmarks).set_index("task")

# Collect results

df = pd.concat(
    [
        dask_res_parquet_temp.duration,
        dask_res_csv_temp.duration,
        dask_res_single_csv_temp.duration,
    ],
    axis=1,
    keys=["dask-parquet", "dask-csv", "dask-single-csv"],
)

print(df)
