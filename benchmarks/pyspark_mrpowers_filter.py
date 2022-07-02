from pyspark.sql.functions import col
from helpers import benchmark, get_results
import sys
import pandas as pd

dataset = sys.argv[1]

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("mrpowers").getOrCreate()


def q1(df):
    return df.filter(col("id4") == 48).count()


def q2(df):
    return df.filter(col("id4") < 30).count()


def q3(df):
    return df.filter(col("v3") < 71.1).count()


def q4(df):
    return df.filter((col("id5") < 48) & (col("v3") < 25.2)).count()


def q5(df):
    return df.filter((col("id2") == "id001") & (col("id5") == 48)).count()


# Parquet

pyspark_parquet_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

parquet_path = f"./data/mrpowers-h2o/groupby-{dataset}/parquet-pyspark"

df = spark.read.parquet(parquet_path)
benchmark(q1, df=df, benchmarks=pyspark_parquet_benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=pyspark_parquet_benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=pyspark_parquet_benchmarks, name="q3")
benchmark(q4, df=df, benchmarks=pyspark_parquet_benchmarks, name="q4")
benchmark(q5, df=df, benchmarks=pyspark_parquet_benchmarks, name="q5")

pyspark_res_parquet_temp = get_results(pyspark_parquet_benchmarks).set_index("task")

print(pyspark_res_parquet_temp)

# CSVs

path = f"./data/mrpowers-h2o/groupby-{dataset}/csv"
df = spark.read.csv(path, header="true")

pyspark_csv_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=df, benchmarks=pyspark_csv_benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=pyspark_csv_benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=pyspark_csv_benchmarks, name="q3")
benchmark(q4, df=df, benchmarks=pyspark_csv_benchmarks, name="q4")
benchmark(q5, df=df, benchmarks=pyspark_csv_benchmarks, name="q5")

pyspark_res_csv_temp = get_results(pyspark_csv_benchmarks).set_index("task")

# Single CSV

path = (
    f"./data/mrpowers-h2o/groupby-{dataset}/single-csv/mrpowers-groupby-{dataset}.csv"
)

df = spark.read.csv(path, header="true")

pyspark_single_csv_benchmarks = {
    "duration": [],  # in seconds
    "task": [],
}

benchmark(q1, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q3")
benchmark(q4, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q4")
benchmark(q5, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q5")

pyspark_res_single_csv_temp = get_results(pyspark_single_csv_benchmarks).set_index(
    "task"
)

print(pyspark_res_single_csv_temp)

# Collect results

df = pd.concat(
    [
        pyspark_res_parquet_temp.duration,
        pyspark_res_csv_temp.duration,
        pyspark_res_single_csv_temp.duration,
    ],
    axis=1,
    keys=["pyspark-parquet", "pyspark-csv", "pyspark-single-csv"],
)

print(df)
