# You need to set the $H2O_PROJECT_PATH variable before running this script
# Something like this works: export H2O_PROJECT_PATH=$HOME/Documents/code/forked/db-benchmark

mkdir -p data/h2o/groupby-datagen_1e7_1e2_0_0/csv
(cd data/h2o/groupby-datagen_1e7_1e2_0_0/csv; Rscript $H2O_PROJECT_PATH/_data/groupby-datagen.R 1e7 1e2 0 1)

#mkdir -p data/h2o/join-datagen_1e7_0_0_0/csv
#(cd data/h2o/join-datagen_1e7_0_0_0/csv; Rscript $H2O_PROJECT_PATH/_data/join-datagen.R 1e7 0 0 0)
