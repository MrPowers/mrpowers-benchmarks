import pytest

from benchmarks.dask_h2o_groupby_queries import *
import dask.dataframe as dd
import beavis
import numpy as np


def test_q1():
    ddf = dd.read_csv("data/h20_groupby_sample.csv")
    res = q1(ddf)
    df = pd.DataFrame.from_dict({"v1": [10, 4, 6], "id1": ["id001", "id002", "id003"]})
    beavis.assert_pd_equality(res, df, check_index=False, check_dtype=False)


def test_q2():
    ddf = dd.read_csv("data/h20_groupby_sample.csv")
    res = q2(ddf)
    df = pd.DataFrame.from_dict(
        {
            "v1": [4, 6, 4, 0, 3, 3],
            "id1": ["id001", "id001", "id002", "id002", "id003", "id003"],
            "id2": ["id01", "id02", "id01", "id02", "id02", "id03"],
        }
    )
    beavis.assert_pd_equality(res, df, check_index=False, check_dtype=False)


def test_q3():
    ddf = dd.read_csv("data/h20_groupby_sample.csv")
    res = q3(ddf)
    df = pd.DataFrame.from_dict(
        {
            "v1": [6, 10, 1, 3, 0],
            "v3": [54.4, 53.075, 70.1, 49.3, 2.0],
            "id3": ["id1", "id2", "id5", "id6", "id7"],
        }
    )
    beavis.assert_pd_equality(res, df, check_index=False, check_dtype=False)


def test_q4():
    ddf = dd.read_csv("data/h20_groupby_sample.csv")
    res = q4(ddf)
    df = pd.DataFrame.from_dict(
        {
            "v1": [0.0, 3.0, 1.0, 2.0, 3.0, 2.0, 4.0, 1.0, 3.0, 1.0],
            "v2": [7.0, 10.0, 6.0, 3.0, 12.0, 0.0, 11.0, 6.0, 14.0, 3.0],
            "v3": [2.0, 49.3, 70.1, 78.2, 7.3, 98.2, 48.4, 57.7, 61.6, 24.1],
            "id4": [2, 5, 13, 14, 15, 23, 52, 54, 56, 77],
        }
    )
    beavis.assert_pd_equality(res, df, check_index=False, check_dtype=False)


def test_q5():
    ddf = dd.read_csv("data/h20_groupby_sample.csv")
    res = q5(ddf)
    df = pd.DataFrame.from_dict(
        {
            "v1": [8, 6, 6],
            "v2": [36, 19, 17],
            "v3": [115.4, 197.6, 183.9],
            "id6": [100, 200, 300],
        }
    )
    beavis.assert_pd_equality(res, df, check_index=False, check_dtype=False)


def test_q7():
    ddf = dd.read_csv("data/h20_groupby_sample.csv")
    res = q7(ddf)
    df = pd.DataFrame.from_dict(
        {"range_v1_v2": [3, 1, -5, -7, -7], "id3": ["id1", "id2", "id5", "id6", "id7"]}
    )
    beavis.assert_pd_equality(res, df, check_index=False, check_dtype=False)


def test_q8():
    ddf = dd.read_csv("data/h20_groupby_sample.csv")
    res = q8(ddf)
    df = pd.DataFrame.from_dict(
        {
            "v3": [57.7, 48.4, 78.2, 70.1, 98.2, 61.6],
            "id6": [100, 100, 200, 200, 300, 300],
        }
    )
    beavis.assert_pd_equality(res, df, check_index=False, check_dtype=False)


# def test_q9():
# ddf = dd.read_csv("data/h20_groupby_sample.csv")
# res = q9(ddf)
# print("***")
# print(res)
# df = pd.DataFrame.from_dict({
# "r2": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
# "id2": ["id01", "id01", "id01", "id01", "id02", "id02", "id02", "id02", "id03", "id03"],
# "id4": [5, 13, 15, 54, 2, 14, 52, 56, 23, 77],
# })
# beavis.assert_pd_equality(res, df, check_index=False, check_dtype=False)
