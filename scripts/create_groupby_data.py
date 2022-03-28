import random
import math

# Purpose of this script is to create the h2o groupby dataset
# h2o script is created with R and only outputs a single file, so doesn't scale well
# This script outputs multiples files, so it scales well for larger datasets

# nrows is the number of rows the script should output in total
# ngroups is the number of "groups", which are used in several ways
# let's look at the columns in the data to better understand the groups

def create_groupby_data(nrows, ngroups, output_dirname):
    # N & K are the variable names used in other scripts
    # nrows is N
    # ngroups is K
    for group_num in range(1, ngroups+1):
        f = open(f"{output_dirname}/mrpowers-groupby-{group_num:05}.csv", "a")
        f.write("id1,id2,id3,id4,id5,id6,v1,v2,v3\n")
        for file_row in range(1, int(nrows/ngroups)+1):
            id1 = group_num
            id2 = int(math.ceil(file_row * ngroups / nrows * ngroups))
            # N = file_row * ngroups
            # K = ngroups
            id3 = random.randint(file_row, nrows / ngroups)
            id4 = random.randint(1, 100)
            id5 = random.randint(1, 100)
            id6 = random.randint(1, 10_000)
            v1 = random.randint(1, 5)
            v2 = random.randint(1, 15)
            v3 = round(random.uniform(1, 100), 6)
            f.write(f"id{id1:03},id{id2:03},id{id3:0>10},{id4},{id5},{id6},{v1},{v2},{v3}\n")
        f.close()

# TODO: make the output_dirname directory if it doesn't already exist

# 1e7
create_groupby_data(10_000_000, 100, "data/mrpowers-h2o/groupby-1e7/csv")

# 1e8
# create_groupby_data(100_000_000, 100, "data/mrpowers-h2o/groupby-1e8/csv")

# 1e9
# create_groupby_data(1_000_000_000, 100, "data/mrpowers-h2o/groupby-1e9/csv")

