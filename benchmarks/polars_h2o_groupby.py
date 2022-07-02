import polars as pl
from helpers import benchmark, get_results
import sys
import glob
import pandas as pd

dataset = sys.argv[1]


def q1(df):
    return df.groupby("id1").agg(pl.sum("v1")).collect()


def q2(df):
    return df.groupby(["id1", "id2"]).agg(pl.sum("v1")).collect()


def q3(df):
    return df.groupby("id3").agg([pl.sum("v1"), pl.mean("v3")]).collect()


def q4(df):
    return (
        df.groupby("id4").agg([pl.mean("v1"), pl.mean("v2"), pl.mean("v3")]).collect()
    )


def q5(df):
    return df.groupby("id6").agg([pl.sum("v1"), pl.sum("v2"), pl.sum("v3")]).collect()


def q6(df):
    return (
        df.groupby(["id4", "id5"])
        .agg([pl.median("v3").alias("v3_median"), pl.std("v3").alias("v3_std")])
        .collect()
    )


def q7(df):
    return (
        df.groupby("id3")
        .agg([(pl.max("v1") - pl.min("v2")).alias("range_v1_v2")])
        .collect()
    )


def q8(df):
    return (
        df.drop_nulls("v3")
        .sort("v3", reverse=True)
        .groupby("id6")
        .agg(pl.col("v3").head(2).alias("largest2_v3"))
        .explode("largest2_v3")
        .collect()
    )


def q9(df):
    return (
        df.groupby(["id2", "id4"])
        .agg((pl.pearson_corr("v1", "v2") ** 2).alias("r2"))
        .collect()
    )


def q10(df):
    return (
        df.groupby(["id1", "id2", "id3", "id4", "id5", "id6"])
        .agg([pl.sum("v3").alias("v3"), pl.count("v1").alias("count")])
        .collect()
    )


# Parquet

# path = f"./data/mrpowers-h2o/groupby-{dataset}/parquet-pyspark"
# all_files = glob.glob(path + "/*.parquet")

# print(all_files)

# ldf = pl.concat(list(pl.scan_parquet(f) for f in all_files))

# polars_parquet_benchmarks = {
# "duration": [],  # in seconds
# "task": [],
# }

# benchmark(q1, df=ldf, benchmarks=polars_parquet_benchmarks, name="q1")

# polars_res_parquet_temp = get_results(polars_parquet_benchmarks).set_index("task")

# print(polars_res_parquet_temp)


# CSVs

path = f"./data/mrpowers-h2o/groupby-{dataset}/csv"
all_files = glob.glob(path + "/*.csv")

ldf = pl.concat(list(pl.scan_csv(f) for f in all_files))

polars_csv_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=ldf, benchmarks=polars_csv_benchmarks, name="q1")
benchmark(q2, df=ldf, benchmarks=polars_csv_benchmarks, name="q2")
benchmark(q3, df=ldf, benchmarks=polars_csv_benchmarks, name="q3")
benchmark(q4, df=ldf, benchmarks=polars_csv_benchmarks, name="q4")
benchmark(q5, df=ldf, benchmarks=polars_csv_benchmarks, name="q5")
benchmark(q6, df=ldf, benchmarks=polars_csv_benchmarks, name="q6")
benchmark(q7, df=ldf, benchmarks=polars_csv_benchmarks, name="q7")
benchmark(q8, df=ldf, benchmarks=polars_csv_benchmarks, name="q8")
benchmark(q9, df=ldf, benchmarks=polars_csv_benchmarks, name="q9")
# benchmark(q10, df=ldf, benchmarks=polars_csv_benchmarks, name="q10")

polars_res_csv_temp = get_results(polars_csv_benchmarks).set_index("task")

print(polars_res_csv_temp)

# Single CSV

path = (
    f"./data/mrpowers-h2o/groupby-{dataset}/single-csv/mrpowers-groupby-{dataset}.csv"
)

with pl.StringCache():
    df_csv = pl.read_csv(
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

df_csv_lazy = df_csv.lazy()


polars_single_csv_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q1")
benchmark(q2, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q2")
benchmark(q3, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q3")
benchmark(q4, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q4")
benchmark(q5, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q5")
benchmark(q6, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q6")
benchmark(q7, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q7")
benchmark(q8, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q8")
benchmark(q9, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q9")
# benchmark(q10, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q10")

polars_res_single_csv_temp = get_results(polars_single_csv_benchmarks).set_index("task")

print(polars_res_single_csv_temp)

# Collect results

df = pd.concat(
    [
        # polars_res_parquet_temp.duration,
        polars_res_csv_temp.duration,
        polars_res_single_csv_temp.duration,
    ],
    axis=1,
    keys=["polars-csv", "polars-single-csv"],
)

print(df)
