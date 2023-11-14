# MrPowers Benchmarks

This repo performs benchmarking analysis on common datasets with popular query engines like pandas, Polars, DataFusion, and PySpark.

It draws inspiration from the [h2o benchmarks](https://github.com/h2oai/db-benchmark) but also includes different types of queries and uses some different execution methodologies (e.g. modern file formats).

The h2o benchmarks have been a great contribution to the data community, but they've made some decisions that aren't as relevant for modern workflows, see [this section](https://github.com/MrPowers/mrpowers-benchmarks#h2o-benchmark-methodology) for more details.

Most readers of this repo are interested in the benchmarking results and don't actually want to reproduce the benchmarking results themselves.  In any case, this repo makes it easy for readers to reproducing the results themselves.  This is particularily useful if you'd like to run the benchmarks on a specific set of hardware.

This repo provides clear instructions on how to generate the datasets and descriptions of the results, so you can easily gain intuition about the actual benchmarks that are run.

## Benchmarking is hard

It's difficult to build accurate benchmarks.  Runtimes depends on the hardware, software versions, and data setup.

Accurate benchmarks are even harder when comparing different technologies.  Certain frameworks will perform better with different files sizes and file formats.  This benchmarking analysis tries to give a fair representation on the range of outcomes that are possible given the most impactful inputs.

The benchmarks presented in this repo should not be interpreted as definitive results.  They're runtimes for specific data tasks, on one type of hardware, with a specific set of software versions.  The code isn't necessarily optimized (we accept community contributions to restructure code).

The data community should find these benchmarks valuable, caveats aside.

## Why benchmarks are important

Suppose you'd like to find the quickest way to join a 2GB CSV file with a 1GB Parquet file on your local machine.

You may not want to perform an exhaustive analysis yourself.  You'll probably find it easier to look up some benchmarks and make and informed decision on the best alternative.

Trying out 10 different options that require figuring out how to use various different programming languages isn't realistic.  Benchmarks serve to guide users to good options for their uses cases, keeping in mind their time constraints.

Benchmarks can be harmful when they're biased or improperly structured and give misleading conclusions.  Benchmarks should not intentionally or unintentionally misguide readers and towards suboptimal technology choices.

Benchmarks should also pave the way for revolutionary technologies to gain adoption.  When a new query engine figures out how to process data in a faster, more reliable manner, they should be able to quantify improvements to users via benchmarks.  This helps drive adoption.

## h2o benchmark methodology

The h2o benchmarks have certain limitations, as do all benchmarks.

This section explains some of the limitations of the h2o benchmarks, not as a criticism, but to explain the tradeoffs that were made in the h2o benchmarks.  The h2o benchmarks are an excellent contribution to the data community and we should be grateful for the engineers that dedicated their time and effort to make them available.

### No longer updated

The h2o benchmarks are no longer being updated, so we can't see the benchmarking results for new query engines and newer versions.  We also can't see updates when engineers that are specialized in a certain query engine make code fixes.

It's completely reasonable that the h2o benchmarks are no longer maintained.  These were updated for years and the data community benefited from many years of updates.

### Single CSV file

The h2o benchmarks are run on data that's stored in a single CSV file.

This introduces a bias for query engines that are optimzied for running on a single file.  This bias is somewhat offset because the data is persisted in memory before the queries are run.

Remember that CSV is a row based file format and column projection is not supported.  You can't cherry pick certain columns and persist them in memory when running queries.

### Data persisted in memory

The h2o benchmarks persist data in memory, but they are using CSV files, so they need to persist all the data in memory at once.  They can't just persist the columns that are relevant to the query.  Persisting all the columns causes certain queries to error out that wouldn't otherwise have issues if only 2 or 3 columns were persisted.

Persisting data in memory also hides performance benefits of querie engines that are capable of performing parallel I/O.

### Engines that support parallel I/O

Certain query engines are designed to read and write data in parallel - others are not.

Query engine users often care about the entire time it takes to run a query.  How long it takes to read the data, persist it in memory, and execute the query.  The h2o benchmarks only give readers information on how long it takes to actually execute the query.  This is certainly valuable information, but potentially misleading for the 50 GB benchmarks.  Reading 50 GB of data is a significant performance advantage compared to reading a single file.

### Small datasets

The h2o benchmarks only test 0.5 GB, 5 GB, and 50 GB datasets.  They don't test 500 GB or 5 TB datasets.

This introduces a bias for query engines that can run on small datasets, but can't work on larger datsets.

### Distributed engines vs single machine engines

Distributed query engines usually make tradeoffs so they're optimized to be run on a cluster of computers.  The h2o benchmarks are run on a single machine, so they're biased against distributed query engines.

Distributed query engines of course offer a massive benefit for data practitioners.  They can be scaled beyond the limits of a single machine and can be used to run analysis on large datasets.  Including benchmarks on larger datasets with distributed clusters is a good way to highlight the strenghts of query engines that are designed to scale.  This is also a good way to highlight the data volume limits of query engines designed to run on a single machine.

### GPUs vs CPUs

The h2o bencharks include query engines that are designed to be run on CPUs and others that are designed to be run on GPUs.  There are a variety of ways to present benchmarking results from different hardware:

* present the results side-by-side, in the same graph
* present the results in different graphs
* present cost adjusted results

h2o decided to present the results side-by-side, which is reasonable, but there are other ways these results could habe been presented too.

### Query optimization

The h2o benchmarks are well specified, so they don't give engines that have query optimizers a chance to shine.  Engines with a query optimizer will rearrange a poorly specified query and make it run better.  The h2o benchmarks could have included some poorly specified queries to highlight they query optimization strenghts of certain engines.

### Lazy computations and collecting results

Certain query engines support lazy execution, so they don't actually run computations until it's absolutely necessary.  Other query engines eagerly execute computations whenever a command is run.

Lazy computation engines generally split up data and execute computations in parallel.  They don't collect results into a single DataFrame by default because it's usually better to keep the data split up for additional parallelism for downstream computations.

Running a query for a lazy computation engine generally involves two steps:

* actually running the computation
* collecting the subresults into a single DataFrame

Collecting the results into a single DataFrame arguably should not be included in the parallel engine computation runtime.  That's an extra step that's required to get the result, but not usually necessary in a real-world use case.

It can unfortunately be hard to divide a query runtime into different compontents.  Most parallel compute engine query runtimes include both, which is probably misleading.

## Quickstart

This section explains how to download data, create software environments, and run benchmarks for the different execution engines.

### Downloading datasets

* Run `aws s3 cp s3://coiled-datasets/h2o-benchmark/N_1e7_K_1e2_single.csv tmp/` to download the 0.5 GB h2o groupby dataset
* Substitute 1e8 and 1e9 to download the 5 GB and 50 GB datasets

### Polars benchmarks

* Create the `mr-polars` environment with `conda env create -f envs/mr-polars.yml`
* Activate the environment with `conda activate mr-polars`
* Run the Polars benchmarks with `python benchmarks/polars_h2o_groupby_csv.py tmp/N_1e7_K_1e2_single.csv`

You'll get output like this that shows the runtime by h2o groupby queries:

```
task  duration
q1    0.038983
q2    0.117003
q3    0.114049
q4    0.044766
q5    0.140829
q6    0.189151
q7    0.109352
q8    0.817299
q9    0.198762
```

### DataFusion benchmarks

* Create the `mr-datafusion` environment with `conda env create -f envs/mr-datafusion.yml`
* Activate the environment with `conda activate mr-datafusion`
* Run the Polars benchmarks with `python benchmarks/datafusion_h2o_groupby_csv.py tmp/N_1e7_K_1e2_single.csv`

### Dask benchmarks

* Create the `mr-dask` environment with `conda env create -f envs/mr-dask.yml`
* Activate the environment with `conda activate mr-dask`
* Run `python dask_csv_to_parquet.py tmp/N_1e7_K_1e2_single.csv tmp/N_1e7_K_1e2_parquet` to break up the CSV dataset to 100 MB Parquet files
* Run the Dask benchmarks with `python benchmarks/dask_h2o_groupby_csv.py tmp/N_1e7_K_1e2_single.csv`

