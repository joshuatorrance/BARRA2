#!/bin/bash

## CONSTANTS
# Data dirs
GETOBS_DIR=/scratch/hd50/jt4085/get_obs
G3_DIR=/scratch/hd50/sjr548/g3_obs

# Temp working dir to unpack obs to
TEMP_DIR=/scratch/hd50/jt4085/tmp

## SCRIPT
# Create the temp dir
temp_dir=$TEMP_DIR/g3_obs
mkdir -p $temp_dir

# Process the tarballs
for tarball in $G3_DIR/*.tar.gz; do
    cycle=`basename $tarball | head -c 14`
    echo $cycle

    # Create the directory if it doesn't already exist
    getobs_cycle_dir=$GETOBS_DIR/$cycle
    mkdir -pv $getobs_cycle_dir

    # Upack tarball to temp_dir
    echo -ne "\tUnpacking G3 obs..."
    tar xf $tarball -C $temp_dir
    echo -e "done."

    # Merge the g3 dir into the getobs one
    # Don't overwrite anythign in getobs
    # Save changes to log file
    echo -ne "\tMerging G3 into GetObs..."

    log_path=$getobs_cycle_dir/merge_g3.log
    echo "GetObs data merged with G3 data using rsync on" >> $log_path
    date >> $log_path

    rsync -vv --archive --recursive --ignore-existing \
        $temp_dir/* $getobs_cycle_dir >> $log_path

    echo -e "done."

    # Delete unpacked files
    rm -rf $temp_dir/*
done


# Delete the temp dir
rm -rf $temp_dir

echo

