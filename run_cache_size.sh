#!/bin/bash

# Filter time to only output most interesting information
export TIME="time: %E max_mem: %Mkb avg_mem: %Kkb"

# Make Sure cache dir exists and is empty
mkdir -p cache

# Ensure that no data is left from previous runs
make clean

declare -a arr=("1000" "10000" "100000" "1000000")

## now loop through the above array
for cache_size in "${arr[@]}"; do
  filename="data/data_10.csv"

  make clean

  # Run the baseline sql approach
  echo "Running baseline for $cache_size"
  /usr/bin/time python3 main.py --approach baseline --input $filename --output "${base%.*}_baseline_output.csv" -c $cache_size

  # Ensure previous cache is removed
  echo "Running task for $cache_size"
  /usr/bin/time python3 main.py --approach task --input $filename --output "${base%.*}_task_output.csv" -c $cache_size
  echo
done
