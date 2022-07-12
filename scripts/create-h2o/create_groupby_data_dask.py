import numpy as np
import pandas as pd
from dask.distributed import Client, wait
import timeit
import os

from argparse import ArgumentParser


def read_inputs():
    """
    Parse command-line arguments to run create_groupby_data.
    User should provide:
    -N : int | float,
        Number of rows of the dataframe
    -K : int | float
        Number of groups
    -nfiles : int
        Number of output files
    -output_dir : str,
        Output directory.
    """

    parser = ArgumentParser(
        description="Manage create_groupby_data command line arguments"
    )

    parser.add_argument(
        "-n", "--N", dest="N", type=float, default=None, help="Total number fo rows"
    )

    parser.add_argument(
        "-k", "--K", dest="K", type=float, default=None, help="Number of groups"
    )

    parser.add_argument(
        "-nf",
        "--nfiles",
        dest="nfiles",
        type=int,
        default=None,
        help="Number of output files",
    )

    parser.add_argument(
        "-dir",
        "--output_dir",
        dest="dir",
        type=str,
        default="",
        help="Output directory",
    )

    return parser.parse_args()


def create_single_df(N, K, nfiles, i, dir):

    nrows = int(N / nfiles)

    sample_id12 = [f"id{str(x).zfill(3)}" for x in range(1, K + 1)]
    sample_id3 = [f"id{str(x).zfill(10)}" for x in range(1, int(N / K) + 1)]

    id1 = np.random.choice(sample_id12, size=nrows, replace=True)
    id2 = np.random.choice(sample_id12, size=nrows, replace=True)
    id3 = np.random.choice(sample_id3, size=nrows, replace=True)
    id4 = np.random.choice(K, size=nrows, replace=True)
    id5 = np.random.choice(K, size=nrows, replace=True)
    id6 = np.random.choice(int(N / K), size=nrows, replace=True)
    v1 = np.random.choice(5, size=nrows, replace=True)
    v2 = np.random.choice(15, size=nrows, replace=True)
    v3 = np.random.uniform(0, 100, size=nrows)

    df = pd.DataFrame(
        dict(
            zip(
                [f"id{x}" for x in range(1, 7)] + ["v1", "v2", "v3"],
                [id1, id2, id3, id4, id5, id6, v1, v2, v3],
            )
        )
    )

    df.to_csv(
        f"{dir}/groupby-N_{N}_K_{K}_file_{i}.csv",
        index=False,
        float_format="{:.6f}".format,
    )


if __name__ == "__main__":

    args = read_inputs()

    N = int(args.N)
    K = int(args.K)
    nfiles = args.nfiles
    dir = args.dir
    i = range(nfiles)

    check_dir = os.path.isdir(dir)

    if not check_dir:
        os.makedirs(dir)
        print(
            f"The directory {dir} was created and data will be at {os.path.realpath(dir)}"
        )
    else:
        print(f"The data will be located at {os.path.realpath(dir)}")

    with Client() as client:
        tic = timeit.default_timer()
        futures = client.map(
            lambda i: create_single_df(N, K, nfiles, dir, i), range(nfiles)
        )
        wait(futures)
        toc = timeit.default_timer()
        total_time = toc - tic
        print(f"Creating this data took: {total_time:.2f} s")
