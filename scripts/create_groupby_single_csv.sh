
# 1e7
mkdir -p ./data/mrpowers-h2o/groupby-1e7/single-csv/
awk '(NR == 1) || (FNR > 1)' ./data/mrpowers-h2o/groupby-1e7/csv/*.csv > ./data/mrpowers-h2o/groupby-1e7/single-csv/mrpowers-groupby-1e7.csv
