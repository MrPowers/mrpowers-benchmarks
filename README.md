# MrPowers Benchmarks

This repo performs benchmarking analysis on popular databases and query engies.

It includes notebooks with the [h20ai/db-benchmark](https://github.com/h2oai/db-benchmark) queries for various query engines.

The h20ai is somewhat limited because it only tests groupby and join queries, so this repo also provides benchmark analyses for these types of queries:

* common queries
* filtering
* I/O
* multiple operations (e.g. filtering then groupby then join)

The benchmarks in this repo are also easy to reproduce locally.  There are clear instructions on how to generate the datasets and descriptions of the results, so you can easily gain intuition about the acutal benchmark that was run.

## Benchmarking is hard

It's difficult to build accurate benchmarks.  Runtimes depends on the hardware, software versions, and data setup.

The benchmarks presented in this repo should not be interpreted as definitive results.  They're runtimes for specific data tasks, on one type of hardware, with a set of version dependencies.  The code isn't necessarily optimized (we accept community contributions to restructure code).

The data community should find these benchmarks valuable, caveats aside.

## Why benchmarks are important

Suppose you'd like to find the quickest way to join to 2GB CSV files on your local machine.

You may not want to perform an exhaustive analysis yourself.  You'll probably find it easier to look up some benchmarks and make and informed decision on the best alternative.

Trying out 10 different options that require figuring out how to use various different programming languages isn't realistic.  Benchmarks serve to guide users to good options for their uses cases, keeping in mind their time constraints.

## Setup

Running the notebooks is a bit onerous because you need to create datasets on your local machine.  You'll need to run some scripts to generate the datasets that are used by the notebooks.  The notebooks assume the files are stored in the `data/` directory of this project.

### Generating h2o CSV datasets

Here's how to generate the h2o datasets.

* Run `conda env create -f envs/rscript.yml` to create an environment with R
* Activate the environment with `conda activate rscript`
* Open a R session by typing `R` in your Terminal
* Install the required package with `install.packages("data.table")`
* Exit the R session with `quit()`
* Respond with `y` when asked `Save workspace image? [y/n/c]` (not sure if this is needed)
* Clone the [db-benchmark](https://github.com/h2oai/db-benchmark) repo
* `cd` into the `_data` directory and run commands like `Rscript groupby-datagen.R 1e7 1e2 0 0` to generate the data files

You can generate the data files in the right directory of this project by running some shell scripts.

* Set the `H2O_PROJECT_PATH` environment variable (here's how I did it on my machine: `export H2O_PROJECT_PATH=~/Documents/code/forked/db-benchmark`).
* Run the shell script to generate the data files with `bash scripts/create_h2o_data.sh`.

The script will create files in these paths:

```
data/
  h2o/
    G1_1e7_1e2_0_0/
      csv/
        G1_1e7_1e2_0_0.csv
```

### Generating h20 Parquet datasets

We'll use Dask to convert these CSV datasets to Parquet datasets.

Create an environment with Dask installed by running `conda env create -f envs/mr-dask.yml`.

Activate the environment with `conda activate mr-dask`.

Open Jupyter with `jupyter lab`.

Run all the cells in the `create-h2o-parquet` notebook that's in the `notebooks/` directory of this repo.



