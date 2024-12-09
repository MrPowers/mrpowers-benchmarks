from helpers import benchmark, get_results
import polars as pl


def q1(dfs):
	x, small, medium, large = dfs
	return x.join(small, on="id1").collect()	


def q2(dfs):
    x, small, medium, large = dfs
    return x.join(medium, on="id2").collect()


def q3(dfs):
    x, small, medium, large = dfs
    return x.join(medium, how="left", on="id2").collect()


def q4(dfs):
    x, small, medium, large = dfs
    return x.join(medium, on="id5").collect()
    

def q5(dfs):
    x, small, medium, large = dfs
    return x.join(large, on="id3").collect()
    

def run_benchmarks(dfs):
    benchmarks = {
        "duration": [],
        "task": [],
    }

    benchmark(q1, dfs=dfs, benchmarks=benchmarks, name="q1")
    benchmark(q2, dfs=dfs, benchmarks=benchmarks, name="q2")
    benchmark(q3, dfs=dfs, benchmarks=benchmarks, name="q3")
    benchmark(q4, dfs=dfs, benchmarks=benchmarks, name="q4")
    benchmark(q5, dfs=dfs, benchmarks=benchmarks, name="q5")

    res = get_results(benchmarks).set_index("task")
    return res
