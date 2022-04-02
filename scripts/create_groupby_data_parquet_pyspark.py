import sys

dataset = sys.argv[1]

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("mrpowers").getOrCreate()

if dataset == "1e7":
    df = spark.read.csv("./data/mrpowers-h2o/groupby-1e7/csv", header='true')
    df.repartition(26).write.parquet(
        "./data/mrpowers-h2o/groupby-1e7/parquet-pyspark"
    )
elif dataset == "1e8":
    df = spark.read.csv("./data/mrpowers-h2o/groupby-1e8/csv", header='true')
    df.repartition(301).write.parquet(
        "./data/mrpowers-h2o/groupby-1e8/parquet-pyspark"
    )
# elif dataset == "1e9":
    # # 1e9
    # ddf = dd.read_csv("./data/mrpowers-h2o/groupby-1e9/csv/*.csv")
    # ddf.repartition("100MB").to_parquet(
        # "./data/mrpowers-h2o/groupby-1e9/parquet",
        # engine="pyarrow",
        # compression="snappy",
    # )

