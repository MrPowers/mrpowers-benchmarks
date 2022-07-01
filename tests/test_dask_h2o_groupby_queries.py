import pytest

from benchmarks.dask_h2o_groupby_queries import *
import dask.dataframe as dd
import beavis


def test_q1():
    ddf = dd.read_csv("data/h20_groupby_sample.csv")
    res = q1(ddf)
    df = pd.DataFrame.from_dict({"v1": [10, 4, 6], "id1": ["id001", "id002", "id003"]})
    beavis.assert_pd_equality(res, df, check_index=False, check_dtype=False)


# def test_q2():
    # ddf = dd.read_csv("data/h20_groupby_sample.csv")
    # res = q2(ddf)
    # print(res)

