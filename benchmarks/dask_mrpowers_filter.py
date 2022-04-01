import dask.dataframe as dd
import pandas as pd
import dask
from helpers import benchmark, get_results
import sys

print("dask version: %s" % dask.__version__)

dataset = sys.argv[1]

def q1(ddf):
    return len(ddf.loc[ddf["id4"] == 48])


def q2(ddf):
    return len(ddf.loc[ddf["id4"] < 30])


def q3(ddf):
    return len(ddf.loc[ddf["v3"] < 71.1])


def q4(ddf):
    return len(ddf.loc[(ddf["id5"] < 48) & (ddf["v3"] < 25.2)])


def q5(ddf):
    return len(ddf.loc[(ddf["id2"] == "id001") & (ddf["id5"] == 48)])


# Parquet

dask_parquet_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

parquet_path = f"./data/mrpowers-h2o/groupby-{dataset}/parquet"

ddf1 = dd.read_parquet(
    parquet_path, columns=["id4"], engine="pyarrow"
)
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

# CSV

dask_data = dd.read_csv(f"./data/mrpowers-h2o/groupby-{dataset}/csv/*.csv")

dask_csv_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=dask_data, benchmarks=dask_csv_benchmarks, name="q1")
benchmark(q2, df=dask_data, benchmarks=dask_csv_benchmarks, name="q2")
benchmark(q3, df=dask_data, benchmarks=dask_csv_benchmarks, name="q3")
benchmark(q4, df=dask_data, benchmarks=dask_csv_benchmarks, name="q4")
benchmark(q5, df=dask_data, benchmarks=dask_csv_benchmarks, name="q5")

dask_res_csv_temp = get_results(dask_csv_benchmarks).set_index("task")

# Single CSV

dask_data = dd.read_csv(f"./data/mrpowers-h2o/groupby-{dataset}/single-csv/*.csv")

dask_single_csv_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=dask_data, benchmarks=dask_single_csv_benchmarks, name="q1")
benchmark(q2, df=dask_data, benchmarks=dask_single_csv_benchmarks, name="q2")
benchmark(q3, df=dask_data, benchmarks=dask_single_csv_benchmarks, name="q3")
benchmark(q4, df=dask_data, benchmarks=dask_single_csv_benchmarks, name="q4")
benchmark(q5, df=dask_data, benchmarks=dask_single_csv_benchmarks, name="q5")

dask_res_single_csv_temp = get_results(dask_single_csv_benchmarks).set_index("task")

# Collect results

df = pd.concat([
    dask_res_parquet_temp.duration,
    dask_res_csv_temp.duration,
    dask_res_single_csv_temp.duration,
], axis=1, keys=['dask-parquet', 'dask-csv', "dask-single-csv"])

print(df)
