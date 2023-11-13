# Contributing to MrPowers Benchmarks

## Generating h2o CSV datasets with h2o R code

Here's how to generate the h2o datasets.

* Run `conda env create -f envs/mr-r.yml` to create an environment with R
* Activate the environment with `conda activate mr-r`
* Open a R session by typing `R` in your Terminal
* Install the required package with `install.packages("data.table")`
* Exit the R session with `quit()`
* Respond with `y` when asked `Save workspace image? [y/n/c]` (not sure if this is needed)
* Clone the [db-benchmark](https://github.com/h2oai/db-benchmark) repo
* `cd` into the `_data` directory and run commands like `Rscript groupby-datagen.R 1e7 1e2 0 0` to generate the data files

Move these CSV files to the `~/data` directory on your machine:

```
~/data/
  G1_1e7_1e2_0_0.csv
  G1_1e8_1e2_0_0.csv
  G1_1e9_1e2_0_0.csv
```

Now create the Parquet files.  Create a conda environment with Delta Lake, PySpark, and PyArrow, like [this one](https://github.com/delta-io/delta-examples/blob/master/envs/pyspark-340-delta-240.yml).

Here's the script to convert from CSV to Parquet:

```
def csv_to_parquet(csv_path, parquet_path):
    writer = None
    with pyarrow.csv.open_csv(in_path) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                writer = pq.ParquetWriter(out_path, next_chunk.schema)
            next_table = pa.Table.from_batches([next_chunk])
            writer.write_table(next_table)
    writer.close()
```

Here's how you can create Delta tables from the Parquet files:

```
parquet_path = f"{Path.home()}/data/G1_1e9_1e2_0_0.parquet"
df = spark.read.format("parquet").load(parquet_path)
delta_path = f"{Path.home()}/data/deltalake/G1_1e9_1e2_0_0"
df.write.format("delta").save(delta_path)
```
