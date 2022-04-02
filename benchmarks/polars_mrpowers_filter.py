import polars as pl
from helpers import benchmark, get_results
import sys
import glob
import pandas as pd

dataset = sys.argv[1]


def q1(df):
    return len(df.filter(pl.col("id4") == 48).collect())


def q2(df):
    return len(df.filter(pl.col("id4") < 30).collect())


def q3(df):
    return len(df.filter(pl.col("v3") < 71.1).collect())


def q4(df):
    return len(df.filter((pl.col("id5") < 48) & (pl.col("v3") < 25.2)).collect())


def q5(df):
    return len(df.filter((pl.col("id2") == "id001") & (pl.col("id5") == 48)).collect())

# Parquet

path = f"./data/mrpowers-h2o/groupby-{dataset}/parquet-pyspark"
all_files = glob.glob(path + "/*.parquet")

ldf = pl.concat(list(pl.scan_parquet(f) for f in all_files))

polars_parquet_benchmarks = {
"duration": [],  # in seconds
"task": [],
}

benchmark(q1, df=ldf, benchmarks=polars_parquet_benchmarks, name="q1")
benchmark(q2, df=ldf, benchmarks=polars_parquet_benchmarks, name="q2")
benchmark(q3, df=ldf, benchmarks=polars_parquet_benchmarks, name="q3")
benchmark(q4, df=ldf, benchmarks=polars_parquet_benchmarks, name="q4")
benchmark(q5, df=ldf, benchmarks=polars_parquet_benchmarks, name="q5")

polars_res_parquet_temp = get_results(polars_parquet_benchmarks).set_index("task")

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

polars_res_csv_temp = get_results(polars_csv_benchmarks).set_index("task")

# Single CSV

path = (
    f"./data/mrpowers-h2o/groupby-{dataset}/single-csv/mrpowers-groupby-{dataset}.csv"
)

df_csv_lazy = pl.scan_csv(path)

polars_single_csv_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q1")
benchmark(q2, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q2")
benchmark(q3, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q3")
benchmark(q4, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q4")
benchmark(q5, df=df_csv_lazy, benchmarks=polars_single_csv_benchmarks, name="q5")

polars_res_single_csv_temp = get_results(polars_single_csv_benchmarks).set_index("task")

print(polars_res_single_csv_temp)

# Collect results

df = pd.concat(
    [
        polars_res_parquet_temp.duration,
        polars_res_csv_temp.duration,
        polars_res_single_csv_temp.duration,
    ],
    axis=1,
    keys=["polars-parquet", "polars-csv", "polars-single-csv"],
)

print(df)

