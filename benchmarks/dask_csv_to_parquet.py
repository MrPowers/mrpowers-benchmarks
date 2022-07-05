import dask.dataframe as dd
import sys

csv_path = sys.argv[1]
parquet_path = sys.argv[2]

ddf = dd.read_csv(csv_path)
ddf.repartition("100MB").to_parquet(
    parquet_path, engine="pyarrow", compression="snappy"
)
