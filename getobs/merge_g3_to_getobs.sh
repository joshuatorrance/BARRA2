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

# Use ${month#0} to ensure zero padded month is treated as a decimal number
if [[ $month =~ ^[0-9]{2}$ && 1 -le ${month#0}  && ${month#0} -le 12 ]]; then
    echo "Month: $month"
else
    echo "Month doesn't seem to the valid: $month"
    echo "Usage: ./get_month_from_sam.sh [4 digit year] [2 digit month]"
    exit 1
fi

# Build the paths to process
getobs_dir=$GETOBS_ROOT_DIR/$year/$month
g3_dir=$G3_ROOT_DIR/$year/$month

# Check all the required data is present
# Calculate the number of cycles in the given month
n_days=$(date '+%d' -d "${year}${month}01 0000 + 1 month - 1 second")
n_cycles=$((n_days*4))

# For GetObs check the number of done.copy files
if [ -d $getobs_dir ]; then
    n_donefiles=$(find $getobs_dir -type f -name done.copy | wc -l)

    if [ $n_donefiles -eq $n_cycles ]; then
        echo "Expected number of done files found for GetObs ($n_cycles)."
    else
        echo "Didn't find the expected number of done files for GetObs."
        echo "Only found $n_donefiles instead of $n_cycles."
        echo "Exiting."
        exit 1
    fi
else
    echo "No GetObs directory found for this month."
    echo "Exiting."
    exit 1
fi

# For G3 check the number of tarballs
if [ -d $g3_dir ]; then
    n_tarballs=$(find $g3_dir -type f -name *.tar.gz | wc -l)

    if [ $n_tarballs -eq $n_cycles ]; then
        echo "Expected number of tarballs found for G3 ($n_cycles)."
    else
        echo "Didn't find the expected number of tarballs for G3."
        echo "Only found $n_tarballs instead of $n_cycles."
        echo "Exiting."
        exit 1
    fi
else
    echo "No G3 directory found for this month."
    echo "Exiting."
    exit 1
fi

# Now check to see if we've already merged this month by counting log files.
n_logs=$(find $getobs_dir -type f -name merge_g3.log | wc -l)
if [ $n_logs -gt 0 ]; then
    if [ $n_logs -eq $n_cycles ]; then
        echo "$n_logs log files found for this month, it has already been merged."
        echo "Exiting."
        exit 0
    else
        echo "$n_logs log files found for this month, has already been partially merged?"
        echo "Exiting."
        exit 1
    fi
else
    echo "No log files found for the merge. No previous attempts as expected."
fi

exit 0

# Create the temp dir
temp_dir=$TEMP_DIR/g3_obs
mkdir -p $temp_dir

# Process the tarballs
for tarball in $g3_dir/*/*.tar.gz; do
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

