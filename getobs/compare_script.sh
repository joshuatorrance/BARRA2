#!/bin/bash

# This script is intended to compare obs extracted with GetObs to those
# from G3

## CONSTANTS
GETOBS_DIR=/scratch/hd50/jt4085/get_obs
G3_DIR=/scratch/hd50/sjr548/g3_obs


## SCRIPT
for getobs_cycle_path in $GETOBS_DIR/*; do
    cycle=`basename $getobs_cycle_path`

    getobs_bufr_path=$getobs_cycle_path/bufr
    g3_tarball_path=$G3_DIR/$cycle*.tar.gz
    if [ -f $g3_tarball_path ]; then
#        echo $cycle

        # Calculate number of bufr files
        g3_count=`tar -tvf $G3_DIR/$cycle*.tar.gz | grep "\.bufr" | wc -l`
        getobs_count=`find $getobs_bufr_path -name *.bufr | wc -l`

        # Calculate total size of directory
        # Measuring the G3 size in this way doesn't work
        # Values do match match those you get if you full extract and du
        g3_size=`tar -xzf $g3_tarball_path --to-stdout | wc -c`
        getobs_size=`du -bc $getobs_bufr_path | tail -n 1 | awk '{print $1}'`

        # Print vars on separate lines
#        echo -e "\tG3 count:\t$g3_count"
#        echo -e "\tGetObs count:\t$getobs_count"

#        echo -e "\tG3 size:\t$g3_size"
#        echo -e "\tGetObs size:\t$getobs_size"

#        echo

        # Print all on one line
        echo $cycle $g3_count $getobs_count $g3_size $getobs_size
    fi
done
