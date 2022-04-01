import dask.dataframe as dd
import pandas as pd
import dask
from helpers import benchmark, get_results
import sys

print("dask version: %s" % dask.__version__)

dataset = sys.argv[1]


def q1(ddf):
    return ddf.groupby("id1", dropna=False, observed=True).agg({"v1": "sum"}).compute()


def q2(ddf):
    return (
        ddf.groupby(["id1", "id2"], dropna=False, observed=True)
        .agg({"v1": "sum"})
        .compute()
    )


def q3(ddf):
    return (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "sum", "v3": "mean"})
        .compute()
    )


def q4(ddf):
    return (
        ddf.groupby("id4", dropna=False, observed=True)
        .agg({"v1": "mean", "v2": "mean", "v3": "mean"})
        .compute()
    )


def q5(ddf):
    return (
        ddf.groupby("id6", dropna=False, observed=True)
        .agg({"v1": "sum", "v2": "sum", "v3": "sum"})
        .compute()
    )


def q2(ddf):
    return (
        ddf.groupby(["id1", "id2"], dropna=False, observed=True)
        .agg({"v1": "sum"})
        .compute()
    )


def q3(ddf):
    return (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "sum", "v3": "mean"})
        .compute()
    )


def q4(ddf):
    return (
        ddf.groupby("id4", dropna=False, observed=True)
        .agg({"v1": "mean", "v2": "mean", "v3": "mean"})
        .compute()
    )


def q5(ddf):
    return (
        ddf.groupby("id6", dropna=False, observed=True)
        .agg({"v1": "sum", "v2": "sum", "v3": "sum"})
        .compute()
    )


def q7(ddf):
    return (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "max", "v2": "min"})
        .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["range_v1_v2"]]
        .compute()
    )


def q8(ddf):
    return (
        ddf[~ddf["v3"].isna()][["id6", "v3"]]
        .groupby("id6", dropna=False, observed=True)
        .apply(
            lambda x: x.nlargest(2, columns="v3"),
            meta={"id6": "Int64", "v3": "float64"},
        )[["v3"]]
        .compute()
    )


def q9(ddf):
    return (
        ddf[["id2", "id4", "v1", "v2"]]
        .groupby(["id2", "id4"], dropna=False, observed=True)
        .apply(
            lambda x: pd.Series({"r2": x.corr()["v1"]["v2"] ** 2}),
            meta={"r2": "float64"},
        )
        .compute()
    )


# Parquet

# dask_data = dd.read_parquet(f"./data/mrpowers-h2o/groupby-{dataset}/parquet", engine="pyarrow")

dask_parquet_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

parquet_path = f"./data/mrpowers-h2o/groupby-{dataset}/parquet"

ddf1 = dd.read_parquet(parquet_path, columns=["id1", "v1"], engine="pyarrow")
benchmark(q1, df=ddf1, benchmarks=dask_parquet_benchmarks, name="q1")

ddf2 = dd.read_parquet(
    parquet_path,
    columns=["id1", "id2", "v1"],
    engine="pyarrow",
)
benchmark(q2, df=ddf2, benchmarks=dask_parquet_benchmarks, name="q2")

ddf3 = dd.read_parquet(
    parquet_path,
    columns=["id3", "v1", "v3"],
    engine="pyarrow",
)
benchmark(q3, df=ddf3, benchmarks=dask_parquet_benchmarks, name="q3")

ddf4 = dd.read_parquet(
    parquet_path,
    columns=["id4", "v1", "v2", "v3"],
    engine="pyarrow",
)
benchmark(q4, df=ddf4, benchmarks=dask_parquet_benchmarks, name="q4")

ddf5 = dd.read_parquet(
    parquet_path,
    columns=["id6", "v1", "v2", "v3"],
    engine="pyarrow",
)
benchmark(q5, df=ddf5, benchmarks=dask_parquet_benchmarks, name="q5")

ddf7 = dd.read_parquet(
    parquet_path,
    columns=["id3", "v1", "v2"],
    engine="pyarrow",
)
benchmark(q7, df=ddf7, benchmarks=dask_parquet_benchmarks, name="q7")

ddf8 = dd.read_parquet(
    parquet_path, columns=["id6", "v1", "v2", "v3"], engine="pyarrow"
)
benchmark(q8, df=ddf8, benchmarks=dask_parquet_benchmarks, name="q8")

ddf9 = dd.read_parquet(
    parquet_path,
    columns=["id2", "id4", "v1", "v2"],
    engine="pyarrow",
)
benchmark(q9, df=ddf9, benchmarks=dask_parquet_benchmarks, name="q9")

dask_res_parquet_temp = get_results(dask_parquet_benchmarks).set_index("task")

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
benchmark(q7, df=dask_data, benchmarks=dask_csv_benchmarks, name="q7")
benchmark(q8, df=dask_data, benchmarks=dask_csv_benchmarks, name="q8")
benchmark(q9, df=dask_data, benchmarks=dask_csv_benchmarks, name="q9")

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
benchmark(q7, df=dask_data, benchmarks=dask_single_csv_benchmarks, name="q7")
benchmark(q8, df=dask_data, benchmarks=dask_single_csv_benchmarks, name="q8")
benchmark(q9, df=dask_data, benchmarks=dask_single_csv_benchmarks, name="q9")

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
