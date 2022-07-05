def q1(ddf):
    return len(ddf.loc[ddf["id4"] == 48])


def q2(ddf):
    return len(ddf.loc[ddf["id4"] < 30])


def q3(ddf):
    return len(ddf.loc[ddf["v3"] < 71.1])


def q4(ddf):
    return len(ddf.loc[(ddf["id5"] < 48) & (ddf["v3"] < 25.2)])


def q5(ddf):
    return len(ddf.loc[(ddf["id2"] == "id001") & (ddf["id5"] == 48)])
