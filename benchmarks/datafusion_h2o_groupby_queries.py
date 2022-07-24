def q1(ctx):
    return ctx.sql("select id1, sum(v1) as v1 from x group by id1").collect()


def q2(ctx):
    return ctx.sql("select id1, id2, sum(v1) as v1 from x group by id1, id2").collect()


def q3(ctx):
    return ctx.sql("select count(distinct(id3)) from x").collect()
    # return ctx.sql("select id3, sum(v1) as v1, avg(v3) as v3 from x group by id3").collect()


def q4(ctx):
    return ctx.sql("select id4, avg(v1) as v1, avg(v2) as v2, avg(v3) as v3 from x group by id4").collect()


def q5(ctx):
    return ctx.sql("select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6").collect()


def q6(ctx):
    return ctx.sql("select id4, id5, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id4, id5").collect()


def q7(ctx):
    return ctx.sql("select id3, max(v1)-min(v2) as range_v1_v2 from x group by id3").collect()


def q8(ctx):
    return ctx.sql("select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x where v3 is not null) sub_query where order_v3 <= 2").collect()


def q9(ctx):
    return ctx.sql("select id2, id4, power(corr(v1, v2), 2) as r2 from x group by id2, id4").collect()


def q10(ctx):
    return ctx.sql("select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6").collect()

