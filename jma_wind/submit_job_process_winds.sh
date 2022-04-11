#!/bin/bash

#PBS -P hd50
#PBS -l ncpus=1
#PBS -l mem=5gb
#PBS -l walltime=24:00:00
#PBS -l storage=gdata/hd50+scratch/hd50+gdata/access
#PBS -l wd

# Variables from parent meta script:
#  start_timestamp
#  end_timestamp
#  bin_size_sec

# Exit if anything goes wrong
set -e

# Parameters
SCRIPT=/g/data/hd50/jt4085/BARRA2/jma_wind/winds_csv_to_bufr.py
OUTPUT_DIR=/scratch/hd50/jt4085/jma_wind/bufr

# Load modules
module load python3/3.8.5
module load eccodes3

# Iterate over the bins
for (( i=$start_timestamp ; i<$end_timestamp ; i+=$bin_size_sec )); do
    # Get the start and end times in the proper format
    start_ts=$i
    end_ts=$(( $i + $bin_size_sec ))

    start_dt=`date --utc --date="@$start_ts" '+%Y%m%dT%H%M'`
    end_dt=`date --utc --date="@$end_ts" '+%Y%m%dT%H%M'`

    # Generate the output file path
    centre_ts=$(( ($end_ts+$start_ts)/2 ))
    year=`date --utc --date="@$centre_ts" '+%Y'`
    month=`date --utc --date="@$centre_ts" '+%m'`
    dt=`date --utc --date="@$centre_ts" '+%Y%m%dT%H%M'`

    output_file="$OUTPUT_DIR/$year/$month/$dt/$dt.bufr"
    temp_output_file=$output_file.temp

    # Skip bin if the output already exists.
    if [[ -f $output_file ]]; then
        echo "Output file already exists. SKIPPING"
        continue
    fi

    # Create the directory for output
    mkdir -p $(dirname $output_file)

    # Run the script
    echo "Running script from $start_dt to $end_dt"
    echo "Output file path: $output_file"
    echo
    python3 $SCRIPT \
        -s $start_dt \
        -e $end_dt \
        -o $temp_output_file

    # Move the temp file to the output file if it exists and is >0
    if [[ -s $temp_output_file ]]; then
        mv $temp_output_file $output_file
    else
        # Delete the temp file is the size is zero
        rm -f $temp_output_file
    fi
done

echo "Script finished at $(date)"

