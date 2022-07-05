# Script downloads all data and runs all Dask benchmarks

echo "Downloading data"
aws s3 cp s3://coiled-datasets/h2o-benchmark/N_1e7_K_1e2_single.csv tmp

echo "Creating Parquet datasets"
python dask_csv_to_parquet.py tmp/N_1e7_K_1e2_single.csv tmp/h2o_groupby_1e7_dask_parquet

echo "Dask h2o 1e7 groupby single CSV"
python benchmarks/dask_h2o_groupby_csv.py tmp/N_1e7_K_1e2_single.csv

echo "Dask h2o 1e7 groupby Parquet"
python benchmarks/dask_h2o_groupby_parquet.py tmp/h2o_groupby_1e7_dask_parquet

echo "Dask mrpowers 1e7 filter single CSV"
python benchmarks/dask_mrpowers_filter_csv.py tmp/N_1e7_K_1e2_single.csv

echo "Dask mrpowers 1e7 filter Parquet"
python benchmarks/dask_mrpowers_filter_parquet.py tmp/h2o_groupby_1e7_dask_parquet

