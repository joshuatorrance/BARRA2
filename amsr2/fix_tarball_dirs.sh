#!/bin/bash

HEAD_DIR=/g/data/hd50/barra2/data/obs/amsr2

working_dir=`pwd`

# Iterate through the data dirs
for year_dir in $HEAD_DIR/*9; do
    year=`basename $year_dir`

    for month_dir in $year_dir/*; do
        month=`basename $month_dir`

        # We've done up to April
        if [[ ${month#0} -le 4 ]]; then
            # Constants with leading 0 are interpreted as octal
            # ${month#0} removes the leading 0
            continue
        fi

        for tarball in $month_dir/*.tar.xz; do
            echo $tarball
            if tar --list --file=$tarball | grep -q 'data/'; then
                # "Correct" structure is flat, i.e. buft files in archive root
                # "Incorrect" structure is
                # data/year/month/YYYYMMDDThhmm/*.bufr

                # Get the datetime string for the re-packing
                filename=`basename $tarball`
                dt_string="${filename%.tar.xz}"

                echo -e "\tTarball structure incorrect."
        
                echo -e "\tUnpacking tarball..."
                tar -xf $tarball -C $month_dir

                echo -e "\tRe-compressing tarball..."
                # Drop into the .bufr directory to simplify tar
                #   I couldn't figure out how to use tar with full paths.
                cd $month_dir/data/$year/$month/$dt_string/
                if tar -cvJf $tarball.temp *.bufr
                then
                    echo -e "\tTarball rebuilt."

                    mv $tarball.temp $tarball
                else
                    echo -e "\tFailed to build tarball..."
                    echo -e "\tCleaning up and moving on."

                    rm $tarball.temp
                fi

                # Return to the previous directory
                cd $working_dir

                echo -e "\tDeleting unpacked files."
                rm -r $month_dir/data
            else
                echo -e "\tTarball structure correct."
            fi
        done
    done
done
