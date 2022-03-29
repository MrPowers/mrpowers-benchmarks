# MrPowers Benchmarks

This repo performs benchmarking analysis on popular databases and query engies.

It includes notebooks with the [h20ai/db-benchmark](https://github.com/h2oai/db-benchmark) queries for various query engines.

The h20ai is somewhat limited because it only tests groupby and join queries, so this repo also provides benchmark analyses for these types of queries:

* filtering
* I/O
* multiple operations (e.g. filtering then groupby then join)

The benchmarks in this repo are also easy to reproduce locally.  There are clear instructions on how to generate the datasets and descriptions of the results, so you can easily gain intuition about the acutal benchmark that was run.

## Benchmarking is hard

It's difficult to build accurate benchmarks.  Runtimes depends on the hardware, software versions, and data setup.

The benchmarks presented in this repo should not be interpreted as definitive results.  They're runtimes for specific data tasks, on one type of hardware, with a set of version dependencies.  The code isn't necessarily optimized (we accept community contributions to restructure code).

The data community should find these benchmarks valuable, caveats aside.

## Why benchmarks are important

Suppose you'd like to find the quickest way to join a 2GB CSV file with a 1 GB Parquet file on your local machine.

You may not want to perform an exhaustive analysis yourself.  You'll probably find it easier to look up some benchmarks and make and informed decision on the best alternative.

Trying out 10 different options that require figuring out how to use various different programming languages isn't realistic.  Benchmarks serve to guide users to good options for their uses cases, keeping in mind their time constraints.

## Setup

Running the notebooks is a bit onerous because you need to create datasets on your local machine.  You'll need to run some scripts to generate the datasets that are used by the notebooks.  The notebooks assume the files are stored in the `data/` directory of this project.

## Generating h20 datasets with MrPowers Python code

The h2o datasets can be generated with Python code that's more scalable than the h2o R code.  The h2o code only outputs a single file and will error out for big datasets.

The MrPowers scripts output multiple files, so they're scalable.

* Run `python scripts/create_groupby_data.py 1e8` to create the CSV datasets
* Run `python scripts/create_groupby_data_parquet.py 1e8` to create the Parquet datasets
* Run `bash scripts/create_groupby_single_csv.sh` to create the single file CSV datasets

The Parquet generation scripts use Dask.

Create an environment with Dask installed by running `conda env create -f envs/mr-dask.yml`.

Activate the environment with `conda activate mr-dask`.

## Running benchmarks

Once the data is created you can run the benchmarks.  Here's how to run the Dask groupby examples on the h2o data for example: `python benchmarks/dask_h2o_groupby.py 1e8`.  This will return a pandas DataFrame with the h2o groupby queries and the runtime by data storage type.

```
task  dask-parquet    dask-csv  dask-single-csv
q1        6.530111   42.375217        68.157102
q2       11.591496   45.914591        60.002330
q3      158.140141  173.817864       186.369429
q4        3.110347   49.357321        74.218600
q5        3.022649   45.040007        78.528167
q7      158.184490  153.350738       170.875564
q8      176.275249  118.200210       160.345228
q9      192.618197   91.034851        97.658833
```

## Generating h2o CSV datasets with h2o R code

Here's how to generate the h2o datasets.

* Run `conda env create -f envs/rscript.yml` to create an environment with R
* Activate the environment with `conda activate rscript`
* Open a R session by typing `R` in your Terminal
* Install the required package with `install.packages("data.table")`
* Exit the R session with `quit()`
* Respond with `y` when asked `Save workspace image? [y/n/c]` (not sure if this is needed)
* Clone the [db-benchmark](https://github.com/h2oai/db-benchmark) repo
* `cd` into the `_data` directory and run commands like `Rscript groupby-datagen.R 1e7 1e2 0 0` to generate the data files

You can generate the data files in the right directory of this project by running a shell script in this project.

* Set the `H2O_PROJECT_PATH` environment variable (here's how I did it on my machine: `export H2O_PROJECT_PATH=~/Documents/code/forked/db-benchmark`).
* Run the shell script to generate the data files with `bash scripts/create_h2o_data.sh`.

The script will create files in these paths:

```
data/
  h2o/
    groupby-datagen_1e7_1e2_0_0/
      csv/
        G1_1e7_1e2_0_0.csv
    join-datagen_1e7_0_0_0/
      csv/
        J1_1e7_1e1_0_0.csv
        J1_1e7_1e4_0_0.csv
        J1_1e7_1e7_0_0.csv
        J1_1e7_NA_0_0.csv
```

