# MrPowers Benchmarks

This repo performs benchmarking analysis on popular databases and query engies.

It's similar to the [h20ai/db-benchmark](https://github.com/h2oai/db-benchmark), but also includes the following benchmark tests:

* common queries
* filtering
* I/O
* multiple operations (e.g. filtering then groupby then join)

The h20ai benchmarks only covers groupby and join queries.

The benchmarks in this repo are also easy to reproduce locally.  There are clear instructions on how to generate the datasets and descriptions of the results, so you can easily gain intuition about the acutal benchmark that was run.

## Benchmarking is hard

It's difficult to build accurate benchmarks.  Runtimes depends on the hardware, software versions, and data setup.

The benchmarks presented in this repo should not be interpreted as definitive results.  They're runtimes for specific data tasks, on one type of hardware, with a set of version dependencies.  The code isn't necessarily optimized (we accept community contributions to restructure code).

The community will find these benchmarks valuable, caveats aside.

## Benchmarks are important

Suppose you'd like to find the quickest way to join to 2GB CSV files on your local machine.

You may not want to perform an exhaustive analysis yourself.  You'll probably find it easier to look up some benchmarks and make and informed decision on the best alternative.

Trying out 10 different options that require figuring out how to use various different programming languages isn't realistic.  Benchmarks serve to guide users to good options for their uses cases, keeping in mind their time constraints.

