from helpers import benchmark, get_results
import daft

print("HELLO")

def q1(df):
    x = df
    return daft.sql("select id1, sum(v1) as v1 from x group by id1").collect()


def q2(df):
    x = df
    return daft.sql("select id1, id2, sum(v1) as v1 from x group by id1, id2").collect()


def q3(df):
    x = df
    return daft.sql("select count(distinct(id3)) from x").collect()
    # return ctx.sql("select id3, sum(v1) as v1, avg(v3) as v3 from x group by id3").collect()


def q4(df):
    x = df
    return daft.sql("select id4, avg(v1) as v1, avg(v2) as v2, avg(v3) as v3 from x group by id4").collect()


def q5(df):
    x = df
    return daft.sql("select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6").collect()


def q6(df):
    x = df
    return daft.sql("select id4, id5, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id4, id5").collect()


def q7(df):
    x = df
    return daft.sql("select id3, max(v1)-min(v2) as range_v1_v2 from x group by id3").collect()


def q8(df):
    x = df
    return daft.sql("select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x where v3 is not null) sub_query where order_v3 <= 2").collect()


def q9(df):
    x = df
    return daft.sql("select id2, id4, power(corr(v1, v2), 2) as r2 from x group by id2, id4").collect()


def q10(df):
    x = df
    return daft.sql("select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6").collect()


def run_benchmarks(df):
    benchmarks = {
        "duration": [],
        "task": [],
    }

    benchmark(q1, df=df, benchmarks=benchmarks, name="q1")
    benchmark(q2, df=df, benchmarks=benchmarks, name="q2")
    # benchmark(q3, df=df, benchmarks=benchmarks, name="q3")
    benchmark(q4, df=df, benchmarks=benchmarks, name="q4")
    benchmark(q5, df=df, benchmarks=benchmarks, name="q5")
    # benchmark(q6, df=df, benchmarks=benchmarks, name="q6")
    # benchmark(q7, df=df, benchmarks=benchmarks, name="q7")
    # benchmark(q8, df=df, benchmarks=benchmarks, name="q8")
    # benchmark(q9, df=df, benchmarks=benchmarks, name="q9")
    benchmark(q10, df=df, benchmarks=benchmarks, name="q10")

    res = get_results(benchmarks).set_index("task")
    return res

