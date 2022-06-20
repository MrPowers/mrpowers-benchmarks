import dask.dataframe as dd
import sys

# dataset = sys.argv[1]

ddf = dd.read_csv("./data/mrpowers-h2o/groupby-1e8/csv/*.csv")
ddf.repartition(5).to_parquet(
    "./tmp/mrpowers-h2o/groupby-1e7/parquet-dask",
    engine="pyarrow",
    compression="snappy",
)


if dataset == "1e7":
    # 1e7
    ddf = dd.read_csv("./data/mrpowers-h2o/groupby-1e7/csv/*.csv")
    ddf.repartition("100MB").to_parquet(
        "./data/mrpowers-h2o/groupby-1e7/parquet",
        engine="pyarrow",
        compression="snappy",
    )
elif dataset == "1e8":
    # 1e8
    ddf = dd.read_csv("./data/mrpowers-h2o/groupby-1e8/csv/*.csv")
    ddf.repartition("100MB").to_parquet(
        "./data/mrpowers-h2o/groupby-1e8/parquet",
        engine="pyarrow",
        compression="snappy",
    )
elif dataset == "1e9":
    # 1e9
    ddf = dd.read_csv("./data/mrpowers-h2o/groupby-1e9/csv/*.csv")
    ddf.repartition("100MB").to_parquet(
        "./data/mrpowers-h2o/groupby-1e9/parquet",
        engine="pyarrow",
        compression="snappy",
    )
