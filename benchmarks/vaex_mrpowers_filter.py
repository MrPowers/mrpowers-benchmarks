import vaex
from helpers import benchmark, get_results
import sys
import pandas as pd

dataset = sys.argv[1]


def q1(df):
    return len(df[df.id4 == 48])


def q2(df):
    return len(df[df.id4 < 30])


def q3(df):
    return len(df[df.v3 < 71.1])


def q4(df):
    return len(df[(df.id5 < 48) & (df.v3 < 25.2)])


def q5(df):
    return len(df[(df.id2 == "id001") & (df.id5 == 48)])


# Parquet

vaex_parquet_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

df = vaex.open(f"./data/mrpowers-h2o/groupby-{dataset}/parquet/*.parquet")

benchmark(q1, df=df, benchmarks=vaex_parquet_benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=vaex_parquet_benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=vaex_parquet_benchmarks, name="q3")
benchmark(q4, df=df, benchmarks=vaex_parquet_benchmarks, name="q4")
benchmark(q5, df=df, benchmarks=vaex_parquet_benchmarks, name="q5")

vaex_res_parquet_temp = get_results(vaex_parquet_benchmarks).set_index("task")

print(vaex_res_parquet_temp)

# CSVs

# Don't think this is possible

# Single CSV

path = (
    f"./data/mrpowers-h2o/groupby-{dataset}/single-csv/mrpowers-groupby-{dataset}.csv"
)

df = vaex.from_csv(path)

vaex_single_csv_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=df, benchmarks=vaex_single_csv_benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=vaex_single_csv_benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=vaex_single_csv_benchmarks, name="q3")
benchmark(q4, df=df, benchmarks=vaex_single_csv_benchmarks, name="q4")
benchmark(q5, df=df, benchmarks=vaex_single_csv_benchmarks, name="q5")

vaex_res_single_csv_temp = get_results(vaex_single_csv_benchmarks).set_index("task")

print(vaex_res_single_csv_temp)

# Collect results

df = pd.concat(
    [
        vaex_res_parquet_temp.duration,
        vaex_res_single_csv_temp.duration,
    ],
    axis=1,
    keys=["vaex-parquet", "vaex-single-csv"],
)

print(df)
