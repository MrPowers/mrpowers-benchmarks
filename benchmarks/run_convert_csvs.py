from helpers import csv_to_parquet
import glob
import sys
import os

csvs_path = sys.argv[1]
parquets_path = sys.argv[2]

for csv_path in glob.glob(f"{csvs_path}/*.csv"):
	csv_basename = os.path.basename(csv_path)
	csv_filename, csv_extension = os.path.splitext(csv_basename)
	csv_to_parquet(csv_path, f"{parquets_path}/{csv_filename}.parquet")
