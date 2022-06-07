#!/bin/bash

## PARAMETERS
# Directories
ROOT_DATA_DIR="/scratch/hd50/jt4085/jma_wind"

BUFR_DIRS="${ROOT_DATA_DIR}/GMS-1 ${ROOT_DATA_DIR}/GMS-3 ${ROOT_DATA_DIR}/GMS-4"
BUFR_DIRS="${ROOT_DATA_DIR}/GMS-3 ${ROOT_DATA_DIR}/GMS-4"

OUT_DIR="${ROOT_DATA_DIR}/bufr"

## SCRIPT
for bufr_dir in $BUFR_DIRS; do
    echo `basename $bufr_dir`

    for year_month_dir in $bufr_dir/*; do
        year_month=`basename $year_month_dir`
        year=${year_month:0:4}
        month=${year_month:4:2}
        echo $year $month

        for cycle_tarball_path in $year_month_dir/*.tar.gz; do
            filename=`basename $cycle_tarball_path`
            day=${filename:15:2}
            hour=${filename:17:2}
            echo -e "\t$filename"

            # Unpack the tarball
            tar xf $cycle_tarball_path -C $year_month_dir

            unpacked_dir=$year_month_dir/${filename%%.*}

            # Output directory
            output_dir="$OUT_DIR/$year/$month/$year$month${day}T${hour}00"

            # Copy each bufr to the output directory
            # Add .bufr to the end of the filenames
            for bufr_path in $unpacked_dir/*; do
                bufr_filename=`basename $bufr_path`
                dest_filepath="$output_dir/$bufr_filename.bufr"

                echo -e "\t\t$bufr_filename"

                if [ "$bufr_filename" = "*" ]; then
                    echo -e "\t\t\tDirectory is empty, moving on."
                    continue
                fi

                if [ -e $dest_filepath ]; then
                    echo -e "\t\t\tDest file already exists. Skipping."
                else
                    echo -e "\t\t\tCopying file."
                    mkdir -p $output_dir
                    cp $bufr_path $dest_filepath
                fi
            done

            # Cleanup the unpacked tarball
            rm -r $unpacked_dir
        done
    done
done

echo "Script finished at $(date)"
