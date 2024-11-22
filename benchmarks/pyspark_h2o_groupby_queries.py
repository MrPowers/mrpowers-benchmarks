import pyspark.sql.functions as F


def q1(df):
    return df.groupBy("id1").sum("v1").collect()


def q2(df):
    return df.groupby("id1", "id2").agg(F.sum("v1")).collect()


def q3(df):
    return df.groupby("id3").agg(F.sum("v1"), F.mean("v3")).collect()


def q4(df):
    return df.groupby("id4").agg(F.mean("v1"), F.mean("v2"), F.mean("v3")).collect()


def q5(df):
    return df.groupby("id6").agg(F.sum("v1"), F.sum("v2"), F.sum("v3")).collect()


def q6(df):
    return (
        df.groupby(["id4", "id5"])
        .agg(F.mean("v3").alias("v3_mean"), F.stddev("v3").alias("v3_std"))
        .collect()
    )


def q7(df):
    return (
        df.groupby("id3")
        .agg((F.max("v1") - F.min("v2")).alias("range_v1_v2"))
        .collect()
    )


def q8(df):
    return df.drop_nulls("v3").group_by("id6").agg(col("v3").top_k(2).alias("largest2_v3")).explode("largest2_v3").collect()
    # ans = spark.sql(
    #     "select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x where v3 is not null) sub_query where order_v3 <= 2"
    # )
    # return ans.collect()


def q9(df):
    ans = spark.sql(
        "select id2, id4, pow(corr(v1, v2), 2) as r2 from x group by id2, id4"
    )
    return ans.collect()


# def q10(df):
# ans = spark.sql("select sum(v3) as sum_v3, count(v1) as count from x group by id1, id2, id3, id4, id5, id6")
# return ans.collect()
