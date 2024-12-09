from helpers import csv_to_parquet
import glob
import sys
import os
import polars as pl

parquets_path = sys.argv[1]

for parquet_path in glob.glob(f"{parquets_path}/*.parquet"):
	df = pl.scan_parquet(parquet_path, low_memory=True)
	print("***")
	print(parquet_path)
	print(df.collect())
	print(df.collect_schema())

