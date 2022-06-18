# MrPowers Benchmarks

This repo performs benchmarking analysis on common datasets with popular query engines.

It includes the [h20ai/db-benchmark](https://github.com/h2oai/db-benchmark) queries for various query engines, but looks at various ways to store the data on disk, not just a single, uncompressed CSV file.

The h20ai is somewhat limited because it only tests groupby and join queries, so this repo also provides benchmark analyses for these types of queries:

* filtering
* I/O
* multiple operations (e.g. filtering then groupby then join)

The benchmarks in this repo are also easy to reproduce locally.  There are clear instructions on how to generate the datasets and descriptions of the results, so you can easily gain intuition about the actual benchmark that was run.

## Quickstart

* Create the `mr-dask` environment with `conda env create -f envs/mr-dask.yml`
* Activate the environment with `conda activate mr-dask`
* Run `python scripts/create_groupby_data.py 1e7` to create the CSV datasets
* Run `python scripts/create_groupby_data_parquet.py 1e7` to create the Parquet datasets
* Run `bash scripts/create_groupby_single_csv.sh 1e7` to create the single file CSV datasets
* Run the Dask benchmarks with `python benchmarks/dask_h2o_groupby.py 1e7`

You'll get output like this that shows the runtime by h2o groupby query:

```
task  dask-parquet   dask-csv  dask-single-csv
q1        0.794347   4.238871         5.266390
q2        1.211450   4.537796         4.030523
q3        3.334596   9.869100         4.922707
q4        0.349053   4.267194         3.615435
q5        0.287449   4.227683         3.804853
q7        3.177188   9.814380         5.073136
q8        7.545077  16.843491        11.991266
q9        6.961994  14.476193         8.160958
```

Change the argument to `1e8` and run the same commands to generate larger datasets and run benchmarks on 100_000_000 rows of data!

## Provisioning ec2 server

Here are the steps for provisioning an ec2 server, installing dependencies, and running the benchmarks on more powerful, remote machines.

Create a key-pair in ec2.  Once you create a key-pair, download it to your local machine.  My key pair was named powers-h2o-fun.pem.

Go to the AWS console and create an AWS instance.

Move over the private key file to the `.ssh` directory with `cp powers-h2o-fun.pem ~/.ssh`.

Your private key file needs to have specific permissions.  Run these commands to give the file more permissions:

```
chmod g-r powers-h2o-fun.pem
chmod a-r powers-h2o-fun.pem
chmod u+r powers-h2o-fun.pem
```

Find the IP address of your ec2 instance, it should be something like 18.118.128.999.  SSH into the instance with this command:

```
ssh ubuntu@18.118.128.999 -i ~/.ssh/powers-h2o-fun.pem
```

Once you're SSH'd to the box, you want to run the following commands to install conda.

```
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
~/miniconda3/bin/conda init bash
```

You can access SSDs on a lot of ec2 instances, but they aren't formatted or mounted.

Run `df -h` to see the disk free space.

Run `lsblk` to display details about the block devices.  This let's you see the SSDs that are and are not mounted.

Here's how to mount one of the SSDs:

```
sudo mkfs -t ext4 /dev/xvdb
sudo mkdir /scratch
sudo mount /dev/xvdb /scratch/
sudo chmod a+w /scratch/
```

If you stop and start the ec2 instance, you need to remount the SSD (and will lose all the data in the SSD).

Now we want to install the AWS CLI on the ec2 instance, so it's easy to download files from S3 to the SSD.

Before installing AWS, install the unzip package:

```
sudo apt install unzip
```

Install the AWS CLI with these commands:

```
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

Copy the ~/.aws/credentials on your local machine to the ~/.aws/credentials file on the ec2 instance.  This isn't best practice, but it shoudl work.

Download the datasets to the SSD with these commands:

```
aws s3 cp s3://coiled-datasets/h2o-benchmark/N_1e7_K_1e2_single.csv /scratch/

aws s3 cp s3://coiled-datasets/h2o-benchmark/N_1e8_K_1e2_single.csv /scratch/

aws s3 cp s3://coiled-datasets/h2o-benchmark/N_1e9_K_1e2_single.csv /scratch/
```

Activate a conda environment with the correct permissions and run these commands to create the Parquet datasets:

```
import dask.dataframe as dd

dd.read_csv("/scratch/N_1e7_K_1e2_single.csv")
ddf.repartition("100MB").to_parquet("/scratch/N_1e7_K_1e2_parquet", engine="pyarrow", compression="snappy")

dd.read_csv("/scratch/N_1e8_K_1e2_single.csv")
ddf.repartition("100MB").to_parquet("/scratch/N_1e8_K_1e2_parquet", engine="pyarrow", compression="snappy")

dd.read_csv("/scratch/N_1e9_K_1e2_single.csv")
ddf.repartition("100MB").to_parquet("/scratch/N_1e9_K_1e2_parquet", engine="pyarrow", compression="snappy")
```

Now you're ready to run the benchmarks on the ec2 instance from the command line.

Follow [these instructions](https://medium.com/coder-life/practice-2-host-your-website-on-github-pages-39229dc9bb1b) to setup git on the Ubuntu machine.

## Benchmarking is hard

It's difficult to build accurate benchmarks.  Runtimes depends on the hardware, software versions, and data setup.

Accurate benchmarks are even harder when comparing different technologies.  Certain frameworks will perform better with different files sizes and file formats.  This benchmarking analysis tries to give a fair representation on the range of outcomes that are possible given the most impactful inputs.

The benchmarks presented in this repo should not be interpreted as definitive results.  They're runtimes for specific data tasks, on one type of hardware, with a set of version dependencies.  The code isn't necessarily optimized (we accept community contributions to restructure code).

The data community should find these benchmarks valuable, caveats aside.

## Why benchmarks are important

Suppose you'd like to find the quickest way to join a 2GB CSV file with a 1 GB Parquet file on your local machine.

You may not want to perform an exhaustive analysis yourself.  You'll probably find it easier to look up some benchmarks and make and informed decision on the best alternative.

Trying out 10 different options that require figuring out how to use various different programming languages isn't realistic.  Benchmarks serve to guide users to good options for their uses cases, keeping in mind their time constraints.

Benchmarks can be harmful when they're biased or improperly structured and give misleading conclusions.  You don't want to intentionally or unintentionally misguide readers and towards suboptimal technology choices.

## Setup

Running the notebooks is a bit onerous because you need to create datasets on your local machine.  You'll need to run some scripts to generate the datasets that are used by the notebooks.  The notebooks assume the files are stored in the `data/` directory of this project.

## Generating h20 datasets with MrPowers Python code

The h2o datasets can be generated with Python code that's more scalable than the h2o R code.  The h2o code only outputs a single file and will error out for big datasets.

The MrPowers scripts output multiple files, so they're scalable.

* Run `python scripts/create_groupby_data.py 1e8` to create the CSV datasets
* Run `python scripts/create_groupby_data_parquet.py 1e8` to create the Parquet datasets
* Run `bash scripts/create_groupby_single_csv.sh 1e8` to create the single file CSV datasets

The Parquet generation scripts use Dask.

Create an environment with Dask installed by running `conda env create -f envs/mr-dask.yml`.

Activate the environment with `conda activate mr-dask`.

## Running benchmarks

Once the data is created you can run the benchmarks.  Here's how to run the Dask groupby examples on the h2o data for example: `python benchmarks/dask_h2o_groupby.py 1e8`.  This will return a pandas DataFrame with the h2o groupby queries and the runtime by data storage type.

```
task  dask-parquet    dask-csv  dask-single-csv
q1        6.530111   42.375217        68.157102
q2       11.591496   45.914591        60.002330
q3      158.140141  173.817864       186.369429
q4        3.110347   49.357321        74.218600
q5        3.022649   45.040007        78.528167
q7      158.184490  153.350738       170.875564
q8      176.275249  118.200210       160.345228
q9      192.618197   91.034851        97.658833
```

Here are the results of the `python benchmarks/polars-h2o-groupby.py 1e8` command:

```
task   polars-csv  polars-single-csv
q1      10.699258           1.187866
q2      11.287701           1.094340
q3      54.692913           4.411051
q4      43.071231           3.539270
q5      28.731560           2.420679
q6      36.500194           6.299211
q7      52.995038           4.655452
q8      44.739875          15.215694
q9      77.114268           3.222981
```

Here are the results of `python benchmarks/dask_mrpowers_filter.py 1e8`:

```
task  dask-parquet   dask-csv  dask-single-csv
q1        1.523786  39.585544        46.344684
q2        1.642529  45.607886        47.090381
q3        1.867173  40.690858        50.758034
q4        2.252107  38.429522        45.768121
q5        8.916658  43.167999        53.329310
```

## Generating h2o CSV datasets with h2o R code

Here's how to generate the h2o datasets.

* Run `conda env create -f envs/rscript.yml` to create an environment with R
* Activate the environment with `conda activate rscript`
* Open a R session by typing `R` in your Terminal
* Install the required package with `install.packages("data.table")`
* Exit the R session with `quit()`
* Respond with `y` when asked `Save workspace image? [y/n/c]` (not sure if this is needed)
* Clone the [db-benchmark](https://github.com/h2oai/db-benchmark) repo
* `cd` into the `_data` directory and run commands like `Rscript groupby-datagen.R 1e7 1e2 0 0` to generate the data files

You can generate the data files in the right directory of this project by running a shell script in this project.

* Set the `H2O_PROJECT_PATH` environment variable (here's how I did it on my machine: `export H2O_PROJECT_PATH=~/Documents/code/forked/db-benchmark`).
* Run the shell script to generate the data files with `bash scripts/create_h2o_data.sh`.

The script will create files in these paths:

```
data/
  h2o/
    groupby-datagen_1e7_1e2_0_0/
      csv/
        G1_1e7_1e2_0_0.csv
    join-datagen_1e7_0_0_0/
      csv/
        J1_1e7_1e1_0_0.csv
        J1_1e7_1e4_0_0.csv
        J1_1e7_1e7_0_0.csv
        J1_1e7_NA_0_0.csv
```

