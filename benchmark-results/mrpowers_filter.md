# MrPowers Filter Benchmark Results

Computations run on Macbook Air with M1 chip & 8GB of RAM.

1e8 results are displayed.

## Polars

```
task  polars-csv  polars-single-csv
q1      7.107650          11.150514
q2      9.900646          24.620134
q3     14.922941          62.201231
q4     10.203968          19.599013
q5      9.710138          12.165214
```

## Dask

```
task  dask-parquet   dask-csv  dask-single-csv
q1        1.540041  44.713917        88.123291
q2        1.634513  43.228670        77.015435
q3        1.873053  42.112016        81.449069
q4        2.355071  48.308372        60.652701
q5        9.232268  43.541921        65.256507
```

