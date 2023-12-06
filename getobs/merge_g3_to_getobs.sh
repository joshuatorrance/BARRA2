#!/bin/bash

## CONSTANTS
YEAR=2023
MONTH=01

# Data dirs
GETOBS_ROOT_DIR=/g/data/hd50/barra2/data/obs/production
G3_ROOT_DIR=/scratch/hd50/jt4085/get_obs/g3_from_sam

# Temp working dir to unpack obs to
TEMP_DIR=/scratch/hd50/jt4085/tmp

## SCRIPT
echo "Script started at `date`"

if [[ $# = 2 ]]; then
    # Get the year and month from the command line
    year=$1
    month=$2
elif [[ $# = 1 && $1 = "last" ]]; then
    # Get the year and month for the most recent data
    # If all is as expected the last dir listed should be the most recent
    most_recent_path=`ls -d $G3_ROOT_DIR/*/* | sort | tail -n 1`
    month=$(basename $most_recent_path)
    year=$(basename $(dirname $most_recent_path))
else
    echo "merge_g3_to_getobs.sh YYYY MM"
    exit 1
fi

# Check the length of year and month and whether they're integers
if [[ $year =~ ^[0-9]{4}$ ]]; then
    echo "Year: $year"
else
    echo "Year doesn't seem to the valid: $year"
    echo "Usage: ./get_month_from_sam.sh [4 digit year] [2 digit month]"
    exit 1
fi

if [[ $month =~ ^[0-9]{2}$ && 1 -le $month  && $month -le 12 ]]; then
    echo "Month: $month"
else
    echo "Month doesn't seem to the valid: $month"
    echo "Usage: ./get_month_from_sam.sh [4 digit year] [2 digit month]"
    exit 1
fi

# Build the paths to process
getobs_dir=$GETOBS_ROOT_DIR/$year/$month
g3_dir=$G3_ROOT_DIR/$year/$month/*


# Create the temp dir
temp_dir=$TEMP_DIR/g3_obs
mkdir -p $temp_dir

# Process the tarballs
for tarball in $g3_dir/*.tar.gz; do
    cycle=`basename $tarball | head -c 14`
    echo $cycle

    # Create the directory if it doesn't already exist
    getobs_cycle_dir=$getobs_dir/$cycle
    mkdir -pv $getobs_cycle_dir

    # Does the log file already exist?
    # Skip this cycle if it does
    log_path=$getobs_cycle_dir/merge_g3.log
    if [ -f $log_path ]; then
        echo -e "\tCycle already merged. Skipping."
        continue
    fi

    # Upack tarball to temp_dir
    echo -ne "\tUnpacking G3 obs..."
    tar xf $tarball -C $temp_dir
    echo -e "done."

    # Merge the g3 dir into the getobs one
    # Don't overwrite anything in getobs
    # Save changes to log file
    echo -ne "\tMerging G3 into GetObs..."

    echo "GetObs data merged with G3 data using rsync on" >> $log_path
    date >> $log_path

    rsync -vv --archive --recursive --ignore-existing \
        $temp_dir/bufr/* $getobs_cycle_dir/bufr >> $log_path

    echo -e "done."

    # Delete unpacked files
    rm -rf $temp_dir/*
done


# Delete the temp dir
rm -rf $temp_dir

echo

echo "Script finished at `date`"

