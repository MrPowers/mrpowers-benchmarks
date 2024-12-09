import time
import pandas as pd
import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv


def benchmark(f, dfs, benchmarks, name, **kwargs):
    """Benchmark the given function against the given DataFrame.

    Parameters
    ----------
    f: function to benchmark
    dfs: DataFrames used in query
    benchmarks: container for benchmark results
    name: task name

    Returns
    -------
    Duration (in seconds) of the given operation
    """
    start_time = time.time()
    ret = f(dfs, **kwargs)
    benchmarks["duration"].append(time.time() - start_time)
    benchmarks["task"].append(name)
    print(f"{name} took: {benchmarks['duration'][-1]} seconds")
    return benchmarks["duration"][-1]


def get_results(benchmarks):
    """Return a pandas DataFrame containing benchmark results."""
    return pd.DataFrame.from_dict(benchmarks)


def csv_to_parquet(csv_path, parquet_path):
    writer = None
    with pyarrow.csv.open_csv(csv_path) as reader:
        for next_chunk in reader:
            if next_chunk is None:
                break
            if writer is None:
                writer = pyarrow.parquet.ParquetWriter(parquet_path, next_chunk.schema)
            next_table = pyarrow.Table.from_batches([next_chunk])
            writer.write_table(next_table)
    writer.close()
