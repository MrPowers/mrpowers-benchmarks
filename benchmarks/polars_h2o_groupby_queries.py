import polars as pl


def q1(df):
    return df.group_by("id1").agg(pl.sum("v1")).collect()


def q2(df):
    return df.group_by(["id1", "id2"]).agg(pl.sum("v1")).collect()


def q3(df):
    return df.group_by("id3").agg([pl.sum("v1"), pl.mean("v3")]).collect()


def q4(df):
    return (
        df.group_by("id4").agg([pl.mean("v1"), pl.mean("v2"), pl.mean("v3")]).collect()
    )


def q5(df):
    return df.group_by("id6").agg([pl.sum("v1"), pl.sum("v2"), pl.sum("v3")]).collect()


def q6(df):
    return (
        df.group_by(["id4", "id5"])
        .agg([pl.median("v3").alias("v3_median"), pl.std("v3").alias("v3_std")])
        .collect()
    )


def q7(df):
    return (
        df.groupby("id3")
        .agg([(pl.max("v1") - pl.min("v2")).alias("range_v1_v2")])
        .collect()
    )


def q8(df):
    return (
        df.drop_nulls("v3")
        .sort("v3", reverse=True)
        .groupby("id6")
        .agg(pl.col("v3").head(2).alias("largest2_v3"))
        .explode("largest2_v3")
        .collect()
    )


def q9(df):
    return (
        df.groupby(["id2", "id4"])
        .agg((pl.pearson_corr("v1", "v2") ** 2).alias("r2"))
        .collect()
    )


def q10(df):
    return (
        df.groupby(["id1", "id2", "id3", "id4", "id5", "id6"])
        .agg([pl.sum("v3").alias("v3"), pl.count("v1").alias("count")])
        .collect()
    )
