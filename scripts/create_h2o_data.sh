mkdir -p data/h2o/G1_1e7_1e2_0_0/csv
cd data/h2o/G1_1e7_1e2_0_0/csv
Rscript $H2O_PROJECT_PATH/_data/groupby-datagen.R 1e7 1e2 0 0
