#!/bin/bash

if [[ "$1" == "1e7" ]]
then
  mkdir -p ./data/mrpowers-h2o/groupby-1e7/single-csv/
  awk '(NR == 1) || (FNR > 1)' ./data/mrpowers-h2o/groupby-1e7/csv/*.csv > ./data/mrpowers-h2o/groupby-1e7/single-csv/mrpowers-groupby-1e7.csv
fi

if [[ "$1" == "1e8" ]]
then
  mkdir -p ./data/mrpowers-h2o/groupby-1e8/single-csv/
  awk '(NR == 1) || (FNR > 1)' ./data/mrpowers-h2o/groupby-1e8/csv/*.csv > ./data/mrpowers-h2o/groupby-1e8/single-csv/mrpowers-groupby-1e8.csv
fi

if [ "$1" == "1e9" ]
then
  mkdir -p ./data/mrpowers-h2o/groupby-1e9/single-csv/
  awk '(NR == 1) || (FNR > 1)' ./data/mrpowers-h2o/groupby-1e9/csv/*.csv > ./data/mrpowers-h2o/groupby-1e9/single-csv/mrpowers-groupby-1e9.csv
fi

