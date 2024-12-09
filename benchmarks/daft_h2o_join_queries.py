import daft
from helpers import benchmark, get_results
from daft.sql import SQLCatalog

def q1(dfs):
    query = """
SELECT x.id1, x.id2, x.id3, x.id4 as xid4, small.id4 as smallid4, x.id5, x.id6, x.v1, small.v2
FROM x
INNER JOIN small ON x.id1 = small.id1
    """
    catalog=SQLCatalog({"x": dfs[0], "small": dfs[1], "medium": dfs[1], "large": dfs[2]})
    return daft.sql(query, catalog=catalog).collect()


def q2(dfs):
    query = """
SELECT x.id1 as xid1, medium.id1 as mediumid1, x.id2, x.id3, x.id4 as xid4, medium.id4 as mediumid4, x.id5 as xid5, medium.id5 as mediumid5, x.id6, x.v1, medium.v2
FROM x
INNER JOIN medium ON x.id2 = medium.id2
    """
    return ctx.sql(query).collect()


def q3(dfs):
    query = """
SELECT x.id1 as xid1, medium.id1 as mediumid1, x.id2, x.id3, x.id4 as xid4, medium.id4 as mediumid4, x.id5 as xid5, medium.id5 as mediumid5, x.id6, x.v1, medium.v2
FROM x
LEFT JOIN medium ON x.id2 = medium.id2 
    """
    return ctx.sql(query).collect()


def q4(dfs):
    query = """
SELECT x.id1 as xid1, medium.id1 as mediumid1, x.id2, x.id3, x.id4 as xid4, medium.id4 as mediumid4, x.id5 as xid5, medium.id5 as mediumid5, x.id6, x.v1, medium.v2
FROM x
JOIN medium ON x.id5 = medium.id5
    """
    return ctx.sql(query).collect()


def q5(dfs):
    query = """
SELECT x.id1 as xid1, large.id1 as largeid1, x.id2 as xid2, large.id2 as largeid2, x.id3, x.id4 as xid4, large.id4 as largeid4, x.id5 as xid5, large.id5 as largeid5, x.id6 as xid6, large.id6 as largeid6, x.v1, large.v2
FROM x
JOIN large ON x.id3 = large.id3 
    """
    return ctx.sql(query).collect()

def run_benchmarks(dfs):
    benchmarks = {
        "duration": [],
        "task": [],
    }

    benchmark(q1, dfs=dfs, benchmarks=benchmarks, name="q1")
    # benchmark(q2, df=x, benchmarks=benchmarks, name="q2")
    # benchmark(q3, df=x, benchmarks=benchmarks, name="q3")
    # benchmark(q4, df=x, benchmarks=benchmarks, name="q4")
    # benchmark(q5, df=x, benchmarks=benchmarks, name="q5")

    res = get_results(benchmarks).set_index("task")
    return res
