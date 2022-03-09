#!/bin/bash

# Top directory to work in
HEAD_DIR=/g/data/hd50/barra2/data/obs/amsr2

# Converter script - sets env and runs converter
CONVERSION_SCRIPT=/g/data/hd50/jt4085/BARRA2/amsr2/run_converter.sh

# Threading
N_THREADS=10
thread_count=0

# Filter with characters about the *
for year_dir in $HEAD_DIR/*8; do
    echo Year: `basename $year_dir`

    for month_dir in $year_dir/*; do
        echo -e "\tMonth:" `basename $month_dir`

        for bin_dir in $month_dir/*; do

            for hdf_file in $bin_dir/*.h5; do
                echo -e "\t\t" `basename $hdf_file`

                bufr_file=$(sed -e "s/.h5/.bufr/g" <<< $hdf_file)

                # Run the script, silence stdout
                $CONVERSION_SCRIPT -i $hdf_file -o $bufr_file > /dev/null &
                
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
