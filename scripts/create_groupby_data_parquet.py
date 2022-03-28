import dask.dataframe as dd

# 1e7
ddf = dd.read_csv("./data/mrpowers-h2o/groupby-1e7/csv/*.csv")
ddf.repartition("100MB").to_parquet("./data/mrpowers-h2o/groupby-1e7/parquet", engine="pyarrow")

# 1e8


# 1e9
