import pyspark.sql.functions as F
from helpers import benchmark, get_results
import sys
import pandas as pd
from pyspark_h2o_groupby_queries import *
import pyspark

path = sys.argv[1]

from pyspark.sql import SparkSession

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.executor.memory", "10G")
    .config("spark.driver.memory", "25G")
    .config("spark.driver.maxResultSize", "4g")
    .config("spark.sql.shuffle.partitions", "2")
)

spark = builder.getOrCreate()

df = spark.read.format("parquet").load(path)

# instantiate spark session
df.limit(3).show()

benchmarks = {
    "duration": [],
    "task": [],
}

benchmark(q1, df=df, benchmarks=benchmarks, name="q1")
benchmark(q2, df=df, benchmarks=benchmarks, name="q2")
# benchmark(q3, df=df, benchmarks=benchmarks, name="q3")
# benchmark(q4, df=df, benchmarks=benchmarks, name="q4")
# benchmark(q5, df=df, benchmarks=benchmarks, name="q5")
# benchmark(q6, df=df, benchmarks=benchmarks, name="q6")
# benchmark(q7, df=df, benchmarks=benchmarks, name="q7")
# benchmark(q8, df=df, benchmarks=benchmarks, name="q8")
# benchmark(q9, df=df, benchmarks=benchmarks, name="q9")
# benchmark(q10, df=df, benchmarks=benchmarks, name="q10")

res = get_results(benchmarks).set_index("task")
print(res)
