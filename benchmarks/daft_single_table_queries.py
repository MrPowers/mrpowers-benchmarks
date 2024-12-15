from helpers import benchmark, get_results
import daft
from daft.sql import SQLCatalog

# print("setting native runner")
# daft.context.set_runner_native()

def q1(df):
    return daft.sql(
        "select id1, id2, max(v3) as max_v3 from x group by id1, id2 order by max_v3 desc limit 5",
        catalog=SQLCatalog({"x": df})
    ).collect()


def q2(df):
    return daft.sql(
        "select * from x order by v3 desc limit 5",
        catalog=SQLCatalog({"x": df})
    ).collect()


def q3(df):
    return daft.sql(
        "select id1, id2, min(v3) from (select * from x where id4 > 50 and v1 = 1) group by id1, id2",
        catalog=SQLCatalog({"x": df})
    ).collect()


# def q4(df):
#     return daft.sql("select id4, avg(v1) as v1, avg(v2) as v2, avg(v3) as v3 from x group by id4", catalog=SQLCatalog({"x": df})).collect()


# def q5(df):
#     return daft.sql("select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6", catalog=SQLCatalog({"x": df})).collect()


# def q6(df):
#     return daft.sql("select id4, id5, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id4, id5", catalog=SQLCatalog({"x": df})).collect()


# def q7(df):
#     return daft.sql("select id3, max(v1)-min(v2) as range_v1_v2 from x group by id3", catalog=SQLCatalog({"x": df})).collect()


# def q8(df):
#     return daft.sql("select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x where v3 is not null) sub_query where order_v3 <= 2", catalog=SQLCatalog({"x": df})).collect()


# def q9(df):
#     return daft.sql("select id2, id4, power(corr(v1, v2), 2) as r2 from x group by id2, id4", catalog=SQLCatalog({"x": df})).collect()


# def q10(df):
#     return daft.sql("select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6", catalog=SQLCatalog({"x": df})).collect()


def run_benchmarks(dfs):
    df = dfs[0]
    benchmarks = {
        "duration": [],
        "task": [],
    }

    benchmark(q1, df, benchmarks=benchmarks, name="q1")
    benchmark(q2, df, benchmarks=benchmarks, name="q2")
    # benchmark(q3, df, benchmarks=benchmarks, name="q3")
    # benchmark(q4, df, benchmarks=benchmarks, name="q4")
    # benchmark(q5, df, benchmarks=benchmarks, name="q5")
    # benchmark(q6, df, benchmarks=benchmarks, name="q6")
    # benchmark(q7, df, benchmarks=benchmarks, name="q7")
    # benchmark(q8, df, benchmarks=benchmarks, name="q8")
    # benchmark(q9, df, benchmarks=benchmarks, name="q9")

    res = get_results(benchmarks).set_index("task")
    return res
