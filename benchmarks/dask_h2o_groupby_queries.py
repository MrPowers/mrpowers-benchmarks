import dask.dataframe as dd
import pandas as pd
import dask
from helpers import benchmark, get_results


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


