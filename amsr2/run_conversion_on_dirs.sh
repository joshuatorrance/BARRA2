#!/bin/bash

# Top directory to work in
HEAD_DIR=/scratch/hd50/jt4085/amsr2/hdf

# Output directory
OUT_DIR=/g/data/hd50/barra2/data/obs/amsr2

# Converter script - sets env and runs converter
CONVERSION_SCRIPT=/g/data/hd50/jt4085/BARRA2/amsr2/run_converter.sh

# Threading
N_THREADS=10
thread_count=0

# Check command line for a year
if [[ $# -eq 1 ]]; then
    # Get the year from the commandline
    year=$1
else
    # Set the year manually.
    year=22
fi

# Filter with characters about the *
# TODO: add some more checking on the loops to ensure matches are actually dirs
for year_dir in $HEAD_DIR/*$year; do
    y=`basename $year_dir`
    echo "Year: $y"

    for month_dir in $year_dir/*; do
        m=`basename $month_dir`
        echo -e "  Month: $m"

        for bin_dir in $month_dir/*; do
            bin=`basename $bin_dir`
            echo -e "    Bin: $bin"

            d="${bin[@]:6:2}"
            hour="${bin[@]:9:2}"
            minute="${bin[@]:11:2}"
            ts=`date --date="${y}-${m}-${d}T${hour}:${minute}" '+%s'`
            file_str=`date --date="@$(($ts - 3*60*60))" "+%Y%m%d%H%M"`
            for hdf_file in $bin_dir/*$file_str*.h5; do
                hdf_filename=`basename $hdf_file`
                echo -e "      $hdf_filename"

                bufr_filename=$(sed -e "s/.h5/.bufr/g" <<< $hdf_filename)

                out_file="$OUT_DIR/$y/$m/$bin/$bufr_filename"

                # Run the script, silence stdout
                echo "$hdf_file"
                echo "$hdf_file" >&2
                python3 /g/data/hd50/jt4085/BARRA2/amsr2/fix_hdf.py $hdf_file
                $CONVERSION_SCRIPT -i $hdf_file -o $out_file > /dev/null &
                
                # Increment the thread count
                ((thread_count++))

                # Check if we've hit the thread limit
                if [[ thread_count -ge $N_THREADS ]]; then
                    # Wait for threads to close
                    wait

                    thread_count=0
                fi
            done
        done
    done
done

# Wait for any remaining threads to finish
wait

echo "Script Finished at:"
date
