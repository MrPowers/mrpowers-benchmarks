# Script runs all Dask benchmarks

echo "Creating Parquet datasets"
python benchmarks/dask_csv_to_parquet.py $MRPOWERS_DATA_DIRNAME/N_1e7_K_1e2_single.csv $MRPOWERS_DATA_DIRNAME/h2o_groupby_1e7_dask_parquet

echo "Dask h2o 1e7 groupby single CSV"
python benchmarks/dask_h2o_groupby_csv.py $MRPOWERS_DATA_DIRNAME/N_1e7_K_1e2_single.csv

echo "Dask h2o 1e7 groupby Parquet"
python benchmarks/dask_h2o_groupby_parquet.py $MRPOWERS_DATA_DIRNAME/h2o_groupby_1e7_dask_parquet

echo "Dask mrpowers 1e7 filter single CSV"
python benchmarks/dask_mrpowers_filter_csv.py $MRPOWERS_DATA_DIRNAME/N_1e7_K_1e2_single.csv

echo "Dask mrpowers 1e7 filter Parquet"
python benchmarks/dask_mrpowers_filter_parquet.py $MRPOWERS_DATA_DIRNAME/h2o_groupby_1e7_dask_parquet

