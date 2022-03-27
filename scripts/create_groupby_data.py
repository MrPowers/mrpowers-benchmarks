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
    for g in range(1, ngroups+1):
        f = open(f"{output_dirname}/demofile-{g}.csv", "a")
        f.write("id1,id2,id3,id4,id5,id6,v1,v2,v3\n")
        id3_incrementor = random.randint(1,10)
        id3 = 5
        for i in range(1, int(nrows/ngroups)+1):
            id3 = id3 + id3_incrementor
            id2 = int(math.ceil(i * ngroups / nrows * ngroups))
            id4 = random.randint(1,100)
            id5 = random.randint(1, 100)
            id6 = random.randint(1, 10_000)
            v1 = random.randint(1, 5)
            v2 = random.randint(1, 15)
            v3 = round(random.uniform(1,100), 6)
            # TODO: The :03 needs to be updated with the number of digits in ngroups
            f.write(f"id{g:03},id{id2:03},id{id3:0>10},{id4},{id5},{id6},{v1},{v2},{v3}\n")
        f.close()

# TODO: make the tmp directory if it doesn't already exist

# 1e7
create_groupby_data(10_000_000, 100, "tmp")

# 1e8
# create_groupby_data(100_000_000, 100, "tmp")

# 1e9
# create_groupby_data(1_000_000_000, 100, "tmp")

