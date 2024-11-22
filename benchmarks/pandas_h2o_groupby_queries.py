import pandas as pd
from helpers import benchmark, get_results


def q1(df):
    return df.groupby(
        "id1", as_index=False, sort=False, observed=True, dropna=False
    ).agg({"v1": "sum"})


def q2(df):
    return df.groupby(
        ["id1", "id2"], as_index=False, sort=False, observed=True, dropna=False
    ).agg({"v1": "sum"})


def q3(df):
    return df.groupby(
        "id3", as_index=False, sort=False, observed=True, dropna=False
    ).agg({"v1": "sum", "v3": "mean"})


def q4(df):
    return df.groupby(
        "id4", as_index=False, sort=False, observed=True, dropna=False
    ).agg({"v1": "mean", "v2": "mean", "v3": "mean"})


def q5(df):
    return df.groupby(
        "id6", as_index=False, sort=False, observed=True, dropna=False
    ).agg({"v1": "sum", "v2": "sum", "v3": "sum"})


def q6(df):
    return df.groupby(
        ["id4", "id5"], as_index=False, sort=False, observed=True, dropna=False
    ).agg({"v3": ["median", "std"]})


def q7(df):
    return (
        df.groupby("id3", as_index=False, sort=False, observed=True, dropna=False)
        .agg({"v1": "max", "v2": "min"})
        .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["id3", "range_v1_v2"]]
    )


def q8(df):
    return (
        df[~df["v3"].isna()][["id6", "v3"]]
        .sort_values("v3", ascending=False)
        .groupby("id6", as_index=False, sort=False, observed=True, dropna=False)
        .head(2)
    )


def q9(df):
    return df[['id2','id4','v1','v2']].groupby(['id2','id4'], as_index=False, sort=False, observed=True, dropna=False).apply(lambda x: x['v1'].corr(x['v2'])**2).rename(columns={None: "r2"})


def q10(df):
    return df.groupby(
        ["id1", "id2", "id3", "id4", "id5", "id6"],
        as_index=False,
        sort=False,
        observed=True,
        dropna=False,
    ).agg({"v3": "sum", "v1": "size"})


def run_benchmarks(df):
    benchmarks = {
        "duration": [],
        "task": [],
    }
    benchmark(q1, df=df, benchmarks=benchmarks, name="q1")
    benchmark(q2, df=df, benchmarks=benchmarks, name="q2")
    benchmark(q3, df=df, benchmarks=benchmarks, name="q3")
    benchmark(q4, df=df, benchmarks=benchmarks, name="q4")
    benchmark(q5, df=df, benchmarks=benchmarks, name="q5")
    benchmark(q6, df=df, benchmarks=benchmarks, name="q6")
    benchmark(q7, df=df, benchmarks=benchmarks, name="q7")
    # benchmark(q8, df=df, benchmarks=benchmarks, name="q8")
    # benchmark(q9, df=df, benchmarks=benchmarks, name="q9")
    benchmark(q10, df=df, benchmarks=benchmarks, name="q10")

    res = get_results(benchmarks).set_index("task")
    return res
