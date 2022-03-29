import time
import pandas as pd

def benchmark(f, df, benchmarks, name, **kwargs):
    """Benchmark the given function against the given DataFrame.

    Parameters
    ----------
    f: function to benchmark
    df: data frame
    benchmarks: container for benchmark results
    name: task name

    Returns
    -------
    Duration (in seconds) of the given operation
    """
    start_time = time.time()
    ret = f(df, **kwargs)
    benchmarks["duration"].append(time.time() - start_time)
    benchmarks["task"].append(name)
    print(f"{name} took: {benchmarks['duration'][-1]} seconds")
    return benchmarks["duration"][-1]


def get_results(benchmarks):
    """Return a pandas DataFrame containing benchmark results."""
    return pd.DataFrame.from_dict(benchmarks)


