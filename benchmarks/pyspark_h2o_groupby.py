import pyspark.sql.functions as F
from helpers import benchmark, get_results
import sys
import pandas as pd

dataset = sys.argv[1]

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("mrpowers").getOrCreate()


def q1(df):
    return df.groupBy("id1").sum("v1").collect()


def q2(df):
    return df.groupby("id1", "id2").agg(F.sum("v1")).collect()


def q3(df):
    return df.groupby("id3").agg(F.sum("v1"), F.mean("v3")).collect()


def q4(df):
    return (
        df.groupby("id4").agg(F.mean("v1"), F.mean("v2"), F.mean("v3")).collect()
    )


def q5(df):
    return df.groupby("id6").agg(F.sum("v1"), F.sum("v2"), F.sum("v3")).collect()


def q6(df):
    return (
        df.groupby(["id4", "id5"])
        .agg(F.mean("v3").alias("v3_mean"), F.stddev("v3").alias("v3_std"))
        .collect()
    )


def q7(df):
    return (
        df.groupby("id3")
        .agg((F.max("v1") - F.min("v2")).alias("range_v1_v2"))
        .collect()
    )


def q8(df):
    ans = spark.sql("select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x where v3 is not null) sub_query where order_v3 <= 2")
    return ans.collect()


def q9(df):
    ans = spark.sql("select id2, id4, pow(corr(v1, v2), 2) as r2 from x group by id2, id4")
    return ans.collect()


# def q10(df):
    # ans = spark.sql("select sum(v3) as sum_v3, count(v1) as count from x group by id1, id2, id3, id4, id5, id6")
    # return ans.collect()


# Parquet

pyspark_parquet_benchmarks = {
    "duration": [],
    "task": [],
}

parquet_path = f"./data/mrpowers-h2o/groupby-{dataset}/parquet-pyspark"

df = spark.read.parquet(parquet_path)
df.createOrReplaceTempView("x")

benchmark(q1, df=df, benchmarks=pyspark_parquet_benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=pyspark_parquet_benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=pyspark_parquet_benchmarks, name="q3")
benchmark(q4, df=df, benchmarks=pyspark_parquet_benchmarks, name="q4")
benchmark(q5, df=df, benchmarks=pyspark_parquet_benchmarks, name="q5")
benchmark(q6, df=df, benchmarks=pyspark_parquet_benchmarks, name="q6")
benchmark(q7, df=df, benchmarks=pyspark_parquet_benchmarks, name="q7")
benchmark(q8, df=df, benchmarks=pyspark_parquet_benchmarks, name="q8")
benchmark(q9, df=df, benchmarks=pyspark_parquet_benchmarks, name="q9")
# benchmark(q10, df=df, benchmarks=pyspark_parquet_benchmarks, name="q10")

pyspark_res_parquet_temp = get_results(pyspark_parquet_benchmarks).set_index("task")

# CSVs

path = f"./data/mrpowers-h2o/groupby-{dataset}/csv"
df = spark.read.csv(path, header='true', inferSchema=True)

pyspark_csv_benchmarks = {
    "duration": [],
    "task": [],
}

benchmark(q1, df=df, benchmarks=pyspark_csv_benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=pyspark_csv_benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=pyspark_csv_benchmarks, name="q3")
benchmark(q4, df=df, benchmarks=pyspark_csv_benchmarks, name="q4")
benchmark(q5, df=df, benchmarks=pyspark_csv_benchmarks, name="q5")
benchmark(q6, df=df, benchmarks=pyspark_csv_benchmarks, name="q6")
benchmark(q7, df=df, benchmarks=pyspark_csv_benchmarks, name="q7")
benchmark(q8, df=df, benchmarks=pyspark_csv_benchmarks, name="q8")
benchmark(q9, df=df, benchmarks=pyspark_csv_benchmarks, name="q9")
# benchmark(q10, df=df, benchmarks=pyspark_csv_benchmarks, name="q10")

pyspark_res_csv_temp = get_results(pyspark_csv_benchmarks).set_index("task")

# Single CSV

path = (
    f"./data/mrpowers-h2o/groupby-{dataset}/single-csv/mrpowers-groupby-{dataset}.csv"
)

df = spark.read.csv(path, header='true', inferSchema=True)

pyspark_single_csv_benchmarks = {
    "duration": [],
    "task": [],
}

benchmark(q1, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q3")
benchmark(q4, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q4")
benchmark(q5, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q5")
benchmark(q6, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q6")
benchmark(q7, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q7")
benchmark(q8, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q8")
benchmark(q9, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q9")
# benchmark(q10, df=df, benchmarks=pyspark_single_csv_benchmarks, name="q10")

pyspark_res_single_csv_temp = get_results(pyspark_single_csv_benchmarks).set_index("task")

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

