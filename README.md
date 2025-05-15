# MrPowers Benchmarks

This repo performs benchmarking analysis on common datasets with popular query engines like pandas, Polars, DataFusion, and PySpark.

It draws inspiration from the [h2o benchmarks](https://github.com/h2oai/db-benchmark) but also includes different types of queries and uses some different execution methodologies (e.g. modern file formats).

The h2o benchmarks have been a great contribution to the data community, but they've made some decisions that aren't as relevant for modern workflows, see [this section](https://github.com/MrPowers/mrpowers-benchmarks#h2o-benchmark-methodology) for more details.

Most readers of this repo are interested in the benchmarking results and don't actually want to reproduce the benchmarking results themselves.  In any case, this repo makes it easy for readers to reproducing the results themselves.  This is particularily useful if you'd like to run the benchmarks on a specific set of hardware.

This repo provides clear instructions on how to generate the datasets and descriptions of the results, so you can easily gain intuition about the actual benchmarks that are run.

## h2o groupby results on localhost with revised methodology

Here are the results for the h2o groupby queries on the 100 million row dataset (stored in a single Parquet file) for DataFusion and Polars:

![fast_h2o_groupby_1e8](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/groupby-fast.png)

Here are the longer-running group by queries:

![slow_h2o_groupby_1e8](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/groupby-slow.png)

Here are the results for the h2o join queries:

![h2o_join_1e8](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/h2o-join.png)

These queries were run on a Macbook M3 with 16 GB of RAM.

Here's how the benchmarking methdology differs from the h2o benchmarks:

* they include the full query time (h2o benchmarks just include the query time once the data is loaded in memory)
* Parquet files are used instead of CSV

## Single table query results

Here are the results for single table queries:

![single_table_1e8](https://github.com/MrPowers/mrpowers-benchmarks/blob/main/images/single-table.png)

## Running the benchmarks on your machine

Use [falsa](https://github.com/mrpowers-io/falsa) to generate the dataset.

You can use this command: `falsa groupby --path-prefix=~/data --size SMALL --data-format PARQUET`.

Here's how to run the benchmarks in this project: `uv run benchmarks/run_all_groupby.py /Users/matthewpowers/data/G1_1e8_1e2_0_0.parquet`.

The small dataset has 10 million rows and runs quite fast.  The medium dataset (100 million rows) runs slower.  The large dataset (1 billion rows) often causes memory errors and is a good way to stress test query engines on localhost.

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

## Properties of good benchmarks

* accessible dataset
* list all software verions
* list all hardware specs
* open source benchmarking code
* don't use any methdologies that clearly favor one engine without disclosing

## h2o benchmark methodology

The h2o benchmarks have certain limitations, as do all benchmarks.

This section explains some of the limitations of the h2o benchmarks, not as a criticism, but to explain the tradeoffs that were made in the h2o benchmarks.  The h2o benchmarks are an excellent contribution to the data community and we should be grateful for the engineers that dedicated their time and effort to make them available.

### Single CSV file

The h2o benchmarks are run on data that's stored in a single CSV file.

This introduces a bias for query engines that are optimzied for running on a single file.  This bias is somewhat offset because the data is persisted in memory before the queries are run.

Remember that CSV is a row based file format and column projection is not supported.  You can't cherry pick certain columns and persist them in memory when running queries.

CSVs also require for schemas to be inferred or manually specified.  If one engine manually specifies an efficient type (e.g. int32) and another engine infers a less efficient type (e.g. int64) for a given column, then the query comparisons are biased.  Having all engines use the same types defined in the Parquet file provides a more even comparison.

### Data persisted in memory

The h2o benchmarks persist data in memory, but they are using CSV files, so they need to persist all the data in memory at once.  They can't just persist the columns that are relevant to the query.  Persisting all the columns causes certain queries to error out that wouldn't otherwise have issues if only 2 or 3 columns were persisted.

Persisting data in memory also hides performance benefits of querie engines that are capable of performing parallel I/O.

### Presenting overall query runtime is misleading

The h2o benchmarks show the runtimes for each individual query and all the queries summed.  The sum amount can be greatly impacted by one slowly running query, so it's potentially misleading.

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

## Run scripts

```
uv run benchmarks/run_all_groupby.py /Users/matthewpowers/data/G1_1e8_1e2_0_0.parquet
```

```
uv run benchmarks/run_all_single_table.py /Users/matthewpowers/data/G1_1e8_1e2_0_0.parquet
```

```
uv run benchmarks/datafusion_h2o_join.py /Users/matthewpowers/data/J1_1e7_NA_0_0.parquet /Users/matthewpowers/data/J1_1e7_1e1_0_0.parquet /Users/matthewpowers/data/J1_1e7_1e4_0_0.parquet /Users/matthewpowers/data/J1_1e7_1e7_0_0.parquet
```

```
uv run benchmarks/datafusion_h2o_join.py /Users/matthewpowers/data/J1_1e8_NA_0_0.parquet /Users/matthewpowers/data/J1_1e8_1e2_0_0.parquet /Users/matthewpowers/data/J1_1e8_1e5_0_0.parquet /Users/matthewpowers/data/J1_1e8_1e8_0_0.parquet
```

## Convert CSV to Parquet

Here is how you can convert a CSV file to Parquet: `csv_to_parquet(csv_path, parquet_path)`.

Here is how you can convert a directory of CSV files to Parquet files:

```
uv run benchmarks/convert_csvs.py ~/Documents/code/cloned/db-benchmark/_data ~/data
```

## Table overviews

***
/Users/matthewpowers/data/G1_1e8_1e2_0_0.parquet
shape: (100_000_000, 9)
```
┌───────┬───────┬──────────────┬─────┬───┬────────┬─────┬─────┬───────────┐
│ id1   ┆ id2   ┆ id3          ┆ id4 ┆ … ┆ id6    ┆ v1  ┆ v2  ┆ v3        │
│ ---   ┆ ---   ┆ ---          ┆ --- ┆   ┆ ---    ┆ --- ┆ --- ┆ ---       │
│ str   ┆ str   ┆ str          ┆ i64 ┆   ┆ i64    ┆ i64 ┆ i64 ┆ f64       │
╞═══════╪═══════╪══════════════╪═════╪═══╪════════╪═════╪═════╪═══════════╡
│ id016 ┆ id046 ┆ id0000109363 ┆ 88  ┆ … ┆ 146094 ┆ 4   ┆ 6   ┆ 18.837686 │
│ id039 ┆ id087 ┆ id0000466766 ┆ 14  ┆ … ┆ 111330 ┆ 4   ┆ 14  ┆ 46.797328 │
│ id047 ┆ id098 ┆ id0000307804 ┆ 85  ┆ … ┆ 187639 ┆ 3   ┆ 5   ┆ 47.577311 │
│ id043 ┆ id017 ┆ id0000344864 ┆ 87  ┆ … ┆ 256509 ┆ 2   ┆ 5   ┆ 80.462924 │
│ id054 ┆ id027 ┆ id0000433679 ┆ 99  ┆ … ┆ 32736  ┆ 1   ┆ 7   ┆ 15.796662 │
│ …     ┆ …     ┆ …            ┆ …   ┆ … ┆ …      ┆ …   ┆ …   ┆ …         │
│ id080 ┆ id025 ┆ id0000598386 ┆ 43  ┆ … ┆ 56728  ┆ 3   ┆ 9   ┆ 27.47907  │
│ id064 ┆ id012 ┆ id0000844471 ┆ 19  ┆ … ┆ 203895 ┆ 4   ┆ 5   ┆ 5.323666  │
│ id046 ┆ id053 ┆ id0000544024 ┆ 31  ┆ … ┆ 711000 ┆ 5   ┆ 3   ┆ 27.827385 │
│ id081 ┆ id090 ┆ id0000802094 ┆ 53  ┆ … ┆ 57466  ┆ 1   ┆ 15  ┆ 23.319917 │
│ id001 ┆ id057 ┆ id0000141978 ┆ 93  ┆ … ┆ 224681 ┆ 4   ┆ 10  ┆ 91.788497 │
└───────┴───────┴──────────────┴─────┴───┴────────┴─────┴─────┴───────────┘
```
Schema({'id1': String, 'id2': String, 'id3': String, 'id4': Int64, 'id5': Int64, 'id6': Int64, 'v1': Int64, 'v2': Int64, 'v3': Float64})

***
/Users/matthewpowers/data/J1_1e8_NA_0_0.parquet
shape: (100_000_000, 7)
```
┌─────┬────────┬───────────┬───────┬──────────┬─────────────┬───────────┐
│ id1 ┆ id2    ┆ id3       ┆ id4   ┆ id5      ┆ id6         ┆ v1        │
│ --- ┆ ---    ┆ ---       ┆ ---   ┆ ---      ┆ ---         ┆ ---       │
│ i64 ┆ i64    ┆ i64       ┆ str   ┆ str      ┆ str         ┆ f64       │
╞═════╪════════╪═══════════╪═══════╪══════════╪═════════════╪═══════════╡
│ 32  ┆ 57316  ┆ 104012378 ┆ id32  ┆ id57316  ┆ id104012378 ┆ 2.184703  │
│ 17  ┆ 32099  ┆ 103369135 ┆ id17  ┆ id32099  ┆ id103369135 ┆ 26.295686 │
│ 106 ┆ 102270 ┆ 66344514  ┆ id106 ┆ id102270 ┆ id66344514  ┆ 34.744782 │
│ 99  ┆ 51861  ┆ 79312153  ┆ id99  ┆ id51861  ┆ id79312153  ┆ 73.818861 │
│ 11  ┆ 28655  ┆ 51482959  ┆ id11  ┆ id28655  ┆ id51482959  ┆ 66.362821 │
│ …   ┆ …      ┆ …         ┆ …     ┆ …        ┆ …           ┆ …         │
│ 13  ┆ 22767  ┆ 35069816  ┆ id13  ┆ id22767  ┆ id35069816  ┆ 94.651984 │
│ 72  ┆ 99663  ┆ 44320313  ┆ id72  ┆ id99663  ┆ id44320313  ┆ 32.654356 │
│ 110 ┆ 4985   ┆ 22435441  ┆ id110 ┆ id4985   ┆ id22435441  ┆ 75.312469 │
│ 17  ┆ 4136   ┆ 85575483  ┆ id17  ┆ id4136   ┆ id85575483  ┆ 63.577894 │
│ 70  ┆ 75769  ┆ 19286096  ┆ id70  ┆ id75769  ┆ id19286096  ┆ 49.151411 │
└─────┴────────┴───────────┴───────┴──────────┴─────────────┴───────────┘
```
Schema({'id1': Int64, 'id2': Int64, 'id3': Int64, 'id4': String, 'id5': String, 'id6': String, 'v1': Float64})

***
/Users/matthewpowers/data/J1_1e8_1e2_0_0.parquet
shape: (100, 3)
```
┌─────┬───────┬───────────┐
│ id1 ┆ id4   ┆ v2        │
│ --- ┆ ---   ┆ ---       │
│ i64 ┆ str   ┆ f64       │
╞═════╪═══════╪═══════════╡
│ 19  ┆ id19  ┆ 53.89299  │
│ 96  ┆ id96  ┆ 35.865322 │
│ 44  ┆ id44  ┆ 66.587577 │
│ 91  ┆ id91  ┆ 12.940303 │
│ 31  ┆ id31  ┆ 2.883551  │
│ …   ┆ …     ┆ …         │
│ 69  ┆ id69  ┆ 32.144187 │
│ 82  ┆ id82  ┆ 43.766849 │
│ 66  ┆ id66  ┆ 43.799275 │
│ 105 ┆ id105 ┆ 94.711328 │
│ 81  ┆ id81  ┆ 69.904453 │
└─────┴───────┴───────────┘
```
Schema({'id1': Int64, 'id4': String, 'v2': Float64})

***
/Users/matthewpowers/data/J1_1e8_1e5_0_0.parquet
shape: (100_000, 5)
```
┌─────┬────────┬───────┬──────────┬───────────┐
│ id1 ┆ id2    ┆ id4   ┆ id5      ┆ v2        │
│ --- ┆ ---    ┆ ---   ┆ ---      ┆ ---       │
│ i64 ┆ i64    ┆ str   ┆ str      ┆ f64       │
╞═════╪════════╪═══════╪══════════╪═══════════╡
│ 69  ┆ 82839  ┆ id69  ┆ id82839  ┆ 79.322039 │
│ 94  ┆ 65192  ┆ id94  ┆ id65192  ┆ 26.282094 │
│ 27  ┆ 103858 ┆ id27  ┆ id103858 ┆ 51.550879 │
│ 10  ┆ 40683  ┆ id10  ┆ id40683  ┆ 84.647495 │
│ 42  ┆ 5979   ┆ id42  ┆ id5979   ┆ 83.488062 │
│ …   ┆ …      ┆ …     ┆ …        ┆ …         │
│ 42  ┆ 95337  ┆ id42  ┆ id95337  ┆ 32.217377 │
│ 104 ┆ 55177  ┆ id104 ┆ id55177  ┆ 39.670606 │
│ 14  ┆ 46220  ┆ id14  ┆ id46220  ┆ 55.6271   │
│ 31  ┆ 79430  ┆ id31  ┆ id79430  ┆ 52.355275 │
│ 60  ┆ 10612  ┆ id60  ┆ id10612  ┆ 64.503299 │
└─────┴────────┴───────┴──────────┴───────────┘
```
Schema({'id1': Int64, 'id2': Int64, 'id4': String, 'id5': String, 'v2': Float64})

***
/Users/matthewpowers/data/J1_1e8_1e8_0_0.parquet
shape: (100_000_000, 7)
```
┌─────┬───────┬──────────┬───────┬─────────┬────────────┬───────────┐
│ id1 ┆ id2   ┆ id3      ┆ id4   ┆ id5     ┆ id6        ┆ v2        │
│ --- ┆ ---   ┆ ---      ┆ ---   ┆ ---     ┆ ---        ┆ ---       │
│ i64 ┆ i64   ┆ i64      ┆ str   ┆ str     ┆ str        ┆ f64       │
╞═════╪═══════╪══════════╪═══════╪═════════╪════════════╪═══════════╡
│ 107 ┆ 53407 ┆ 81930178 ┆ id107 ┆ id53407 ┆ id81930178 ┆ 75.634881 │
│ 30  ┆ 44458 ┆ 73257490 ┆ id30  ┆ id44458 ┆ id73257490 ┆ 53.043222 │
│ 104 ┆ 89910 ┆ 38361265 ┆ id104 ┆ id89910 ┆ id38361265 ┆ 83.067211 │
│ 14  ┆ 74193 ┆ 47636586 ┆ id14  ┆ id74193 ┆ id47636586 ┆ 59.066146 │
│ 49  ┆ 77202 ┆ 95213755 ┆ id49  ┆ id77202 ┆ id95213755 ┆ 60.308764 │
│ …   ┆ …     ┆ …        ┆ …     ┆ …       ┆ …          ┆ …         │
│ 30  ┆ 26807 ┆ 87061377 ┆ id30  ┆ id26807 ┆ id87061377 ┆ 32.629123 │
│ 14  ┆ 73040 ┆ 40089145 ┆ id14  ┆ id73040 ┆ id40089145 ┆ 54.944554 │
│ 100 ┆ 49006 ┆ 98750911 ┆ id100 ┆ id49006 ┆ id98750911 ┆ 11.757998 │
│ 17  ┆ 89387 ┆ 16056323 ┆ id17  ┆ id89387 ┆ id16056323 ┆ 31.216292 │
│ 53  ┆ 67188 ┆ 12279067 ┆ id53  ┆ id67188 ┆ id12279067 ┆ 0.275111  │
└─────┴───────┴──────────┴───────┴─────────┴────────────┴───────────┘
```
Schema({'id1': Int64, 'id2': Int64, 'id3': Int64, 'id4': String, 'id5': String, 'id6': String, 'v2': Float64})
```
