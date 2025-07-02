import polars as pl
from polars import col
from helpers import benchmark, get_results


def q1(df):
    return df.group_by("id1").agg(pl.sum("v1")).collect(engine="streaming")


def q2(df):
    return df.group_by(["id1", "id2"]).agg(pl.sum("v1")).collect(engine="streaming")


def q3(df):
    return df.group_by("id3").agg([pl.sum("v1"), pl.mean("v3")]).collect(engine="streaming")


def q4(df):
    return (
        df.group_by("id4").agg([pl.mean("v1"), pl.mean("v2"), pl.mean("v3")]).collect(engine="streaming")
    )


def q5(df):
    return df.group_by("id6").agg([pl.sum("v1"), pl.sum("v2"), pl.sum("v3")]).collect(engine="streaming")


def q6(df):
    return (
        df.group_by(["id4", "id5"])
        .agg([pl.median("v3").alias("v3_median"), pl.std("v3").alias("v3_std")])
        .collect(engine="streaming")
    )


def q7(df):
    return (
        df.group_by("id3")
        .agg([(pl.max("v1") - pl.min("v2")).alias("range_v1_v2")])
        .collect(engine="streaming")
    )


def q8(df):
    return df.drop_nulls("v3").group_by("id6").agg(col("v3").top_k(2).alias("largest2_v3")).explode("largest2_v3").collect(engine="streaming")


def q9(df):
    return df.group_by(["id2","id4"]).agg((pl.corr("v1","v2", method="pearson")**2).alias("r2")).collect(engine="streaming")


def q10(df):
    return (
        df.group_by(["id1", "id2", "id3", "id4", "id5", "id6"])
        .agg([pl.sum("v3").alias("v3"), pl.count("v1").alias("count")])
        .collect(engine="streaming")
    )

def run_benchmarks(dfs):
    df = dfs[0]
    polars_benchmarks = {
        "duration": [],  # in seconds
        "task": [],
    }

    benchmark(q1, df, benchmarks=polars_benchmarks, name="q1")
    benchmark(q2, df, benchmarks=polars_benchmarks, name="q2")
    benchmark(q3, df, benchmarks=polars_benchmarks, name="q3")
    benchmark(q4, df, benchmarks=polars_benchmarks, name="q4")
    benchmark(q5, df, benchmarks=polars_benchmarks, name="q5")
    benchmark(q6, df, benchmarks=polars_benchmarks, name="q6")
    benchmark(q7, df, benchmarks=polars_benchmarks, name="q7")
    benchmark(q8, df, benchmarks=polars_benchmarks, name="q8")
    benchmark(q9, df, benchmarks=polars_benchmarks, name="q9")

    polars_res_temp = get_results(polars_benchmarks).set_index("task")
    return polars_res_temp


def run_benchmarks_slow(dfs):
    df = dfs[0]
    polars_benchmarks = {
        "duration": [],  # in seconds
        "task": [],
    }

    benchmark(q10, df, benchmarks=polars_benchmarks, name="q10")

    polars_res_temp = get_results(polars_benchmarks).set_index("task")
    return polars_res_temp
