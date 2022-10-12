#!/bin/bash

# Constants
G3_DIR=/scratch/hd50/sjr548/g3_obs
TEMP_DIR=/scratch/hd50/jt4085/tmp

## SCRIPT
# Create the temp dir
temp_dir=$TEMP_DIR/g3_obs
mkdir -p $temp_dir

# Process the tarballs
for tarball in $G3_DIR/*.tar.gz; do
    echo -n `basename $tarball | head -c 14`

    # Upack tarball to temp_dir
    tar xf $tarball -C $temp_dir

    # Count number of bufr files
    count=`find $temp_dir/bufr -name *.bufr | wc -l`
    echo -n " $count"

    # Size of bufr files
    size=`du -bc $temp_dir/bufr | tail -n 1 | awk '{print $1}'`
    echo " $size"

    # Delete unpacked files
    rm -rf $temp_dir/*
done


# Delete the temp dir
rm -rf $temp_dir

echo

