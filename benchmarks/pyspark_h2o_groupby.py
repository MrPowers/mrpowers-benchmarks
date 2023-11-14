import pyspark.sql.functions as F
from helpers import benchmark, get_results
import sys
import pandas as pd
from pyspark_h2o_groupby_queries import *
from delta import configure_spark_with_delta_pip
import pyspark

path = sys.argv[1]

from pyspark.sql import SparkSession

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.executor.memory", "10G")
    .config("spark.driver.memory", "25G")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.shuffle.partitions", "2")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.format("delta").load(path)

benchmarks = {
    "duration": [],
    "task": [],
}

benchmark(q1, df=df, benchmarks=benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=benchmarks, name="q2")
benchmark(q3, df=df, benchmarks=benchmarks, name="q3")
# benchmark(q4, df=df, benchmarks=benchmarks, name="q4")
# benchmark(q5, df=df, benchmarks=benchmarks, name="q5")
# benchmark(q6, df=df, benchmarks=benchmarks, name="q6")
# benchmark(q7, df=df, benchmarks=benchmarks, name="q7")
# benchmark(q8, df=df, benchmarks=benchmarks, name="q8")
# benchmark(q9, df=df, benchmarks=benchmarks, name="q9")
# benchmark(q10, df=df, benchmarks=benchmarks, name="q10")

res = get_results(benchmarks).set_index("task")
print(res)
