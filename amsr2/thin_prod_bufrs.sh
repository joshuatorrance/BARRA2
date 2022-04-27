#!/bin/bash

# AMSR2's bufr files are too big, slowing down runs.
#
# This script goes through production and thins the AMSR2 bufr files.


## PARAMETERS
# Paths
PROD_DIR=/g/data/hd50/barra2/data/obs/production
FILTER_PATH=/g/data/hd50/jt4085/BARRA2/util/bufr/bufr_thin_3.filter
ARCHIVE_DIR=/scratch/hd50/jt4085/amsr2/concat_bufrs

# Start and end times, inclusive
START_YEAR=2012
END_YEAR=2022

# Multithreading
MAX_THREADS=10

# Expected max thinned size
# - thinned bufrs should probably be less than this.
THINNED_MAX_SIZE=157286400 # 150MB = 150*1024*1024

## FUNCTION
thin_bufr_file() {
    # Initial full bufr filename and symlink dest
    dest_bufr_path=$1/AMSR2_1.bufr
    full_bufr_path=$1/full.bufr
    thinned_bufr_path=$1/thinned.bufr
    archive_bufr_dir=$2
    
    echo -e "\t\t\tThinning $1"
    dest_file_size=`stat -c%s $dest_bufr_path`
    if (( $dest_file_size < $THINNED_MAX_SIZE )); then
         echo -e "\t\t\t\tBUFR already thinned."
    else
        # Copy the full bufr to full.bufr
        cp $dest_bufr_path $full_bufr_path

        # Thin full.bufr to thinned.bufr
        bufr_filter -o $thinned_bufr_path $FILTER_PATH $full_bufr_path

        # Move full.bufr to the archive
        # Don't overwrite the archive file if it already exists
        # Perhaps processing was interrupted?
        mkdir -p $archive_bufr_dir
        if ! [ -e $archive_bufr_dir/full.bufr ]; then
            mv $full_bufr_path $archive_bufr_dir
        else
            echo -e "\t\t\t\tArchive file already exists: $archive_bufr_dir"
        fi

        # Move thinned.bufr to AMSR_1.bufr
        mv $thinned_bufr_path $dest_bufr_path

        echo -e "\t\t\t\tDone $1"
    fi
}


## SCRIPT
thread_count=0
for year_dir in $PROD_DIR/*/; do
    y=`basename $year_dir`
    echo "Year: $y"

    if (( $START_YEAR <= $y )) && (( $y <= $END_YEAR )); then
        for month_dir in $year_dir*/; do
            m=`basename $month_dir`
            echo -e "\tMonth: $m"

            for dt_dir in $month_dir*/; do
                dt=`basename $dt_dir`
                echo -e "\t\tDatetime: $dt"

                amsr_dir=${dt_dir}bufr/amsr
                if [ -d $amsr_dir ]; then
                    # ls ${dt_dir}bufr/amsr/*

                    thinned_bufr_path=$amsr_dir/thinned.bufr

                    archive_bufr_dir=$ARCHIVE_DIR/$y/$m/$dt/

                    thin_bufr_file $amsr_dir $archive_bufr_dir &

                    # Increment the thread count
                    ((thread_count++))

                    # Check if we've hit the thread limit
                    if [[ $thread_count -ge $MAX_THREADS ]]; then
                        # Wait for threads to close
                        echo -e "\t\t\tWaiting for threads to close."
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

# Wait for threads to close
echo -e "\t\t\t Waiting for threads to close."
wait

echo "Script finished at $(date)"
