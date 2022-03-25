import random

def create_groupby_data(nrows, ngroups):
    # N & K are the variable names used in other scripts
    # nrows is N
    # ngroups is K
    for g in range(1, ngroups+1):
        f = open(f"demofile-{g}.csv", "a")
        f.write("id1,id2,id3,id4,id5,id6,v1,v2,v3\n")
        for i in range(1, int(nrows/ngroups)+1):
            # TODO: The :03 needs to be updated with the number of digits in ngroups
            f.write(f"id{g:03},id{int(i/ngroups):03},id{random.randint(1,10):0>10},{random.randint(1,100)},{random.randint(1, 100)},{random.randint(1, 10_000)},{random.randint(1, 5)},{random.randint(1, 15)},{round(random.uniform(1,100), 6)}\n")
        f.close()

create_groupby_data(12, 2)
