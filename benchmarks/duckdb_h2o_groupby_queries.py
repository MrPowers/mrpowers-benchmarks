import duckdb
from helpers import benchmark, get_results
import sys
import pandas as pd

def q1(path):
    return duckdb.sql(f"SELECT id1, sum(v1) as v1 FROM read_parquet('{path}') group by id1;").df()


def q2(path):
    return duckdb.sql(f"select id1, id2, sum(v1) as v1 from read_parquet('{path}') group by id1, id2").df()


def q3(path):
    return duckdb.sql(f"select count(distinct(id3)) from read_parquet('{path}')").df()


def q4(path):
    return duckdb.sql(f"select id4, avg(v1) as v1, avg(v2) as v2, avg(v3) as v3 from read_parquet('{path}') group by id4").df()


def q5(path):
    return duckdb.sql(f"select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from read_parquet('{path}') group by id6").df()


def q6(path):
    return duckdb.sql(f"select id4, id5, median(v3) as median_v3, stddev(v3) as sd_v3 from read_parquet('{path}') group by id4, id5").df()


def q7(path):
    return duckdb.sql(f"select id3, max(v1)-min(v2) as range_v1_v2 from read_parquet('{path}') group by id3").df()


def q8(path):
    return duckdb.sql(f"select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from read_parquet('{path}') where v3 is not null) sub_query where order_v3 <= 2").df()


def q9(path):
    return duckdb.sql(f"select id2, id4, power(corr(v1, v2), 2) as r2 from read_parquet('{path}') group by id2, id4").df()


def q10(path):
    return duckdb.sql(f"select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from read_parquet('{path}') group by id1, id2, id3, id4, id5, id6").df()


paths = [sys.argv[1]]
def run_benchmarks(paths):
    path = paths[0]

    duckdb_benchmarks = {
        "duration": [],  # in seconds
        "task": [],
    }

    benchmark(q1, dfs=path, benchmarks=duckdb_benchmarks, name="q1")
    benchmark(q2, dfs=path, benchmarks=duckdb_benchmarks, name="q2")
    benchmark(q3, dfs=path, benchmarks=duckdb_benchmarks, name="q3")
    benchmark(q4, dfs=path, benchmarks=duckdb_benchmarks, name="q4")
    benchmark(q5, dfs=path, benchmarks=duckdb_benchmarks, name="q5")
    benchmark(q6, dfs=path, benchmarks=duckdb_benchmarks, name="q6")
    benchmark(q7, dfs=path, benchmarks=duckdb_benchmarks, name="q7")
    benchmark(q8, dfs=path, benchmarks=duckdb_benchmarks, name="q8")
    benchmark(q9, dfs=path, benchmarks=duckdb_benchmarks, name="q9")
    # benchmark(q10, dfs=path, benchmarks=duckdb_benchmarks, name="q10")

    duckdb_res_temp = get_results(duckdb_benchmarks).set_index("task")
    print(duckdb_res_temp)
    return duckdb_res_temp


def run_benchmarks_slow(paths):
    path = paths[0]

    duckdb_benchmarks = {
        "duration": [],  # in seconds
        "task": [],
    }

    benchmark(q10, dfs=path, benchmarks=duckdb_benchmarks, name="q10")

    duckdb_res_temp = get_results(duckdb_benchmarks).set_index("task")
    print(duckdb_res_temp)
    return duckdb_res_temp
