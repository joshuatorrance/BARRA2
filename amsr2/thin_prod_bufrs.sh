#!/bin/bash

# AMSR2's bufr files are too big, slowing down runs.
#
# This script goes through production and thins the AMSR2 bufr files.


## PARAMETERS
# Paths
PROD_DIR=/g/data/hd50/barra2/data/obs/production
FILTER_PATH=/g/data/hd50/jt4085/BARRA2/util/bufr/bufr_thin.filter

# Start and end times, inclusive
START_YEAR=2012
END_YEAR=2022

# Multithreading
MAX_THREADS=10


## FUNCTION
thin_bufr_file() {
    # Initial full bufr filename and symlink dest
    dest_bufr_path=$1/AMSR2_1.bufr
    full_bufr_path=$1/full.bufr
    thinned_bufr_path=$1/thinned.bufr
    
    if [ -e $thinned_bufr_path ]; then
         echo -e "\t\t\tBUFR already thinned."
    else
        mv $dest_bufr_path $full_bufr_path
        bufr_filter -o $thinned_bufr_path $FILTER_PATH $full_bufr_path
        ln -s $thinned_bufr_path $dest_bufr_path
    fi
}


## SCRIPT
thread_count=0
for year_dir in $PROD_DIR/*/; do
    y=`basename $year_dir`
    echo "Year: $y"

    if (( $START_YEAR <= $y )) && (( $y <= $END_YEAR )); then
        for month_dir in $year_dir*/; do
            echo -e "\tMonth: $(basename $month_dir)"

            for dt_dir in $month_dir*/; do
                echo -e "\t\tDatetime: $(basename $dt_dir)"

                amsr_dir=${dt_dir}bufr/amsr
                if [ -d $amsr_dir ]; then
                    ls ${dt_dir}bufr/amsr/*

                    thinned_bufr_path=$amsr_dir/thinned.bufr

                    thin_bufr_file $amsr_dir &

                    # Increment the thread count
                    ((thread_count++))

                    # Check if we've hit the thread limit
                    if [[ thread_count -ge $MAX_THREADS ]]; then
                        # Wait for threads to close
                        echo "\t\t\t Waiting for threads to close"
                        wait

                        thread_count=0
                    fi
                else
                    echo -e "\t\t\tNo AMSR dir."
                fi
            done
        done
    else
        echo -e "\tYear outside boundaries, skipping."
    fi
done

echo "Script finished at $(date)"
