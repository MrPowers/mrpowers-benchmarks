# Example usage: bash download_nyc_taxi 2009

mkdir data/nyc-tlc
#aws s3 cp s3://nyc-tlc/trip\ data/yellow_tripdata_2009-01.csv data/nyc-tlc
#aws s3 cp s3://nyc-tlc/trip\ data/yellow_tripdata_2009-02.csv data/nyc-tlc

for VARIABLE in 01 02 03 04 05 06 07 08 09 10 11 12
do
  aws s3 cp s3://nyc-tlc/trip\ data/yellow_tripdata_$1-$VARIABLE.csv data/nyc-tlc
done

