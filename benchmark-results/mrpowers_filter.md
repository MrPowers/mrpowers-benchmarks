# MrPowers Filter Benchmark Results

Computations run on Macbook Air with M1 chip & 8GB of RAM.

## 1e8

*Polars*

```
task  polars-parquet  polars-csv  polars-single-csv
q1          4.147290   16.049853          14.464048
q2          7.025856   23.496531          62.517372
q3         16.354966   76.546058         161.577843
q4          8.573848   41.575043          31.605694
q5          6.021159   12.663761          14.181147
```

*Dask*

```
task  dask-parquet   dask-csv  dask-single-csv
q1        1.540041  44.713917        88.123291
q2        1.634513  43.228670        77.015435
q3        1.873053  42.112016        81.449069
q4        2.355071  48.308372        60.652701
q5        9.232268  43.541921        65.256507
```

*PySpark*

```
task  pyspark-parquet  pyspark-csv  pyspark-single-csv
q1           8.942071    73.299720           80.908305
q2           7.183153    74.775582           78.702317
q3          16.020203   103.221765          106.889338
q4          13.436045   103.330660          108.612671
q5           5.466127    74.781006           68.551979
```

