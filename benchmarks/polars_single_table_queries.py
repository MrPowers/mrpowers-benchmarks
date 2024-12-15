import polars as pl
from polars import col
from helpers import benchmark, get_results


def q1(df):
    x = df
    query = "select id1, id2, max(v3) as max_v3 from x group by id1, id2 order by max_v3 desc limit 5"
    return pl.sql(query).collect()


def q2(df):
    x = df
    query = "select * from x order by v3 desc limit 5"
    return pl.sql(query).collect()



def q3(df):
    x = df
    query = "select id1, id2, min(v3) from x where id4 > 50 and v1 = 1 group by id1, id2"
    return pl.sql(query).collect()


def q4(df):
    x = df
    query = "select id4, avg(v1) as v1, avg(v2) as v2, avg(v3) as v3 from x where id6 < 100 and v2 > 5 group by id4 order by id4 desc"
    return pl.sql(query).collect()


# def q5(df):
#     return df.group_by("id6").agg([pl.sum("v1"), pl.sum("v2"), pl.sum("v3")]).collect()


# def q6(df):
#     return (
#         df.group_by(["id4", "id5"])
#         .agg([pl.median("v3").alias("v3_median"), pl.std("v3").alias("v3_std")])
#         .collect()
#     )


# def q7(df):
#     return (
#         df.group_by("id3")
#         .agg([(pl.max("v1") - pl.min("v2")).alias("range_v1_v2")])
#         .collect()
#     )


# def q8(df):
#     return df.drop_nulls("v3").group_by("id6").agg(col("v3").top_k(2).alias("largest2_v3")).explode("largest2_v3").collect()


# def q9(df):
#     return df.group_by(["id2","id4"]).agg((pl.corr("v1","v2", method="pearson")**2).alias("r2")).collect()


# def q10(df):
#     return (
#         df.group_by(["id1", "id2", "id3", "id4", "id5", "id6"])
#         .agg([pl.sum("v3").alias("v3"), pl.count("v1").alias("count")])
#         .collect()
#     )

def run_benchmarks(dfs):
    df = dfs[0]
    polars_benchmarks = {
        "duration": [],  # in seconds
        "task": [],
    }

    benchmark(q1, df, benchmarks=polars_benchmarks, name="q1")
    benchmark(q2, df, benchmarks=polars_benchmarks, name="q2")
    benchmark(q3, df, benchmarks=polars_benchmarks, name="q3")
    benchmark(q4, df, benchmarks=polars_benchmarks, name="q4")
    # benchmark(q5, df, benchmarks=polars_benchmarks, name="q5")
    # benchmark(q6, df, benchmarks=polars_benchmarks, name="q6")
    # benchmark(q7, df, benchmarks=polars_benchmarks, name="q7")
    # benchmark(q8, df, benchmarks=polars_benchmarks, name="q8")
    # benchmark(q9, df, benchmarks=polars_benchmarks, name="q9")

    polars_res_temp = get_results(polars_benchmarks).set_index("task")
    return polars_res_temp
