from helpers import benchmark, get_results

def q1(ctx):
    query = "select id1, sum(v1) as v1 from x group by id1"
    return ctx.sql(query).collect()


def q2(ctx):
    return ctx.sql("select id1, id2, sum(v1) as v1 from x group by id1, id2").collect()


def q3(ctx):
    return ctx.sql("select id3, sum(v1) as v1, mean(v3) as v3 from x group by id3").collect()


def q4(ctx):
    return ctx.sql("select id4, avg(v1) as v1, avg(v2) as v2, avg(v3) as v3 from x group by id4").collect()


def q5(ctx):
    return ctx.sql("select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6").collect()


def q6(ctx):
    return ctx.sql("select id4, id5, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id4, id5").collect()


def q7(ctx):
    return ctx.sql("select id3, max(v1)-min(v2) as range_v1_v2 from x group by id3").collect()


def q8(ctx):
    sql = """
SELECT id6, largest2_v3
from
  (SELECT id6, v3 as largest2_v3, row_number() over (partition by id6 ORDER BY v3 DESC) as order_v3
  FROM x WHERE v3 is not null) sub_query
  where order_v3 <= 2
    """
    return ctx.sql(sql).collect()


def q9(ctx):
    return ctx.sql("select id2, id4, power(corr(v1, v2), 2) as r2 from x group by id2, id4").collect()


def q10(ctx):
    sql = """
    select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count
    from x
    group by id1, id2, id3, id4, id5, id6
    """
    return ctx.sql(sql).collect()


def run_benchmarks(ctx):
    benchmarks = {
        "duration": [],
        "task": [],
    }

    benchmark(q1, ctx, benchmarks=benchmarks, name="q1")
    benchmark(q2, ctx, benchmarks=benchmarks, name="q2")
    benchmark(q3, ctx, benchmarks=benchmarks, name="q3")
    benchmark(q4, ctx, benchmarks=benchmarks, name="q4")
    benchmark(q5, ctx, benchmarks=benchmarks, name="q5")
    benchmark(q6, ctx, benchmarks=benchmarks, name="q6")
    benchmark(q7, ctx, benchmarks=benchmarks, name="q7")
    benchmark(q8, ctx, benchmarks=benchmarks, name="q8")
    benchmark(q9, ctx, benchmarks=benchmarks, name="q9")

    res = get_results(benchmarks).set_index("task")
    return res


def run_benchmarks_slow(ctx):
    benchmarks = {
        "duration": [],
        "task": [],
    }

    benchmark(q10, ctx, benchmarks=benchmarks, name="q10")

    res = get_results(benchmarks).set_index("task")
    return res
