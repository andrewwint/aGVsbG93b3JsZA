#!/bin/bash

# Filter time to only output most interesting information
export TIME="time: %E max_mem: %Mkb avg_mem: %Kkb"

# Make Sure cache dir exists and is empty
mkdir -p cache

# Ensure that no data is left from previous runs
make clean

for filename in data/*.csv; do
  base=$(basename -- $filename)

  # Run the baseline sql approach
  echo "Running baseline for $base"
  /usr/bin/time python3 main.py --approach baseline --input $filename --output "${base%.*}_baseline_output.csv"

  # Ensure previous cache is removed
  rm -f cache/*
  echo "Running task approach for $base"
  /usr/bin/time python3 main.py --approach task --input $filename --output "${base%.*}_task_output.csv"
  echo
done
