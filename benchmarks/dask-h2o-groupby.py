import dask.dataframe as dd
import pandas as pd
import dask
import time

print("dask version: %s" % dask.__version__)

dask_data = dd.read_parquet("./data/mrpowers-h2o/groupby-1e7/parquet", engine="pyarrow")

dask_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}


def benchmark(f, df, benchmarks, name, **kwargs):
    """Benchmark the given function against the given DataFrame.

    Parameters
    ----------
    f: function to benchmark
    df: data frame
    benchmarks: container for benchmark results
    name: task name

    Returns
    -------
    Duration (in seconds) of the given operation
    """
    start_time = time.time()
    ret = f(df, **kwargs)
    benchmarks["duration"].append(time.time() - start_time)
    benchmarks["task"].append(name)
    print(f"{name} took: {benchmarks['duration'][-1]} seconds")
    return benchmarks["duration"][-1]


def get_results(benchmarks):
    """Return a pandas DataFrame containing benchmark results."""
    return pd.DataFrame.from_dict(benchmarks)


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
    return ddf.groupby("id4", dropna=False, observed=True).agg(
        {"v1": "mean", "v2": "mean", "v3": "mean"}
    ).compute()


def q5(ddf):
    return ddf.groupby("id6", dropna=False, observed=True).agg(
        {"v1": "sum", "v2": "sum", "v3": "sum"}
    ).compute()


def q7(ddf):
    return ddf.groupby("id3", dropna=False, observed=True).agg({"v1": "max", "v2": "min"}).assign(
        range_v1_v2=lambda x: x["v1"] - x["v2"]
    )[["range_v1_v2"]].compute()


def q8(ddf):
    return ddf[~ddf["v3"].isna()][["id6", "v3"]].groupby("id6", dropna=False, observed=True).apply(
        lambda x: x.nlargest(2, columns="v3"), meta={"id6": "Int64", "v3": "float64"}
    )[["v3"]].compute()


def q9(ddf):
    return ddf[["id2", "id4", "v1", "v2"]].groupby(
        ["id2", "id4"], dropna=False, observed=True
    ).apply(
        lambda x: pd.Series({"r2": x.corr()["v1"]["v2"] ** 2}), meta={"r2": "float64"}
    ).compute()


benchmark(q1, df=dask_data, benchmarks=dask_benchmarks, name="q1")
benchmark(q2, df=dask_data, benchmarks=dask_benchmarks, name="q2")
benchmark(q3, df=dask_data, benchmarks=dask_benchmarks, name="q3")
benchmark(q4, df=dask_data, benchmarks=dask_benchmarks, name="q4")
benchmark(q5, df=dask_data, benchmarks=dask_benchmarks, name="q5")
benchmark(q7, df=dask_data, benchmarks=dask_benchmarks, name="q7")
benchmark(q8, df=dask_data, benchmarks=dask_benchmarks, name="q8")
benchmark(q9, df=dask_data, benchmarks=dask_benchmarks, name="q9")

dask_res_temp = get_results(dask_benchmarks).set_index("task")
print(dask_res_temp)
