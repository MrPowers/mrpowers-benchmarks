# MrPowers Benchmarks

This repo performs benchmarking analysis on common datasets with popular query engines.

It includes the [h20ai/db-benchmark](https://github.com/h2oai/db-benchmark) queries, but looks at various ways to store the data on disk, not just a single, uncompressed CSV file.

The h20 benchmarks are limited because they only benchmark groupby and join queries.  This repo also provides benchmark analyses for these types of queries:

* filtering
* I/O
* multiple operations (e.g. filtering then groupby then join)

The benchmarks in this repo are also easy to reproduce locally.  There are clear instructions on how to generate the datasets and descriptions of the results, so you can easily gain intuition about the actual benchmark that was run.

## Quickstart

* Create the `mr-dask` environment with `conda env create -f envs/mr-dask.yml`
* Activate the environment with `conda activate mr-dask`
* Run `aws s3 cp s3://coiled-datasets/h2o-benchmark/N_1e7_K_1e2_single.csv tmp/` to download one of the h2o groupby datasets
* Run `python dask_csv_to_parquet.py tmp/N_1e7_K_1e2_single.csv tmp/N_1e7_K_1e2_parquet` to break up the CSV dataset to 100 MB Parquet files
* Run the Dask benchmarks with `python benchmarks/dask_h2o_groupby_csv.py tmp/N_1e7_K_1e2_single.csv`

You'll get output like this that shows the runtime by h2o groupby query:

```
task   duration
q1     4.762273
q2     4.550942
q3     4.720327
q4     4.006821
q5     4.287993
q7     5.015371
q8    68.201933
q9     8.156151
```

## Benchmarking is hard

It's difficult to build accurate benchmarks.  Runtimes depends on the hardware, software versions, and data setup.

Accurate benchmarks are even harder when comparing different technologies.  Certain frameworks will perform better with different files sizes and file formats.  This benchmarking analysis tries to give a fair representation on the range of outcomes that are possible given the most impactful inputs.

The benchmarks presented in this repo should not be interpreted as definitive results.  They're runtimes for specific data tasks, on one type of hardware, with a set of version dependencies.  The code isn't necessarily optimized (we accept community contributions to restructure code).

The data community should find these benchmarks valuable, caveats aside.

## Why benchmarks are important

Suppose you'd like to find the quickest way to join a 2GB CSV file with a 1 GB Parquet file on your local machine.

You may not want to perform an exhaustive analysis yourself.  You'll probably find it easier to look up some benchmarks and make and informed decision on the best alternative.

Trying out 10 different options that require figuring out how to use various different programming languages isn't realistic.  Benchmarks serve to guide users to good options for their uses cases, keeping in mind their time constraints.

Benchmarks can be harmful when they're biased or improperly structured and give misleading conclusions.  You don't want to intentionally or unintentionally misguide readers and towards suboptimal technology choices.

