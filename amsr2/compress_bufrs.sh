#!/bin/sh

DATA_DIR="/g/data/hd50/barra2/data/obs/amsr2"

for year in `ls $DATA_DIR`
do
    year_dir=$DATA_DIR/$year

    if [[ $1 != $year ]]; then
        echo "Year doesn't match argument, skipping."
        continue
    fi

    for month in `ls $year_dir`
    do
        month_dir=$year_dir/$month

        for bin in `ls $month_dir`
        do
            if [[ $bin != *".tar.gz" ]]
            then
                bin_dir=$month_dir/$bin

                echo $bin_dir
                echo "  Compressing..."

                # Jump into the bin directory to ensure a clean tarball
                cd $bin_dir

                # Output to a temporary file
                out_file="$month_dir/$bin.tar.gz"
                temp_out_file="$out_file.temp"

                tar -cvzf $temp_out_file *.bufr

                # Move the temp file to the true output file
                mv $temp_out_file $out_file

                # Return to the previous directory
                cd -

                if [ $? -eq 0 ]
                then
                    echo "    Done."

                    # If one is feeling brave uncomment the following to
                    # auto-delete the .bufrs after compressing.
                    echo "  Deleting uncompressed .bufr files..." 
                    rm -vr $bin_dir
                    echo "    Done." 
                else
                    echo "    Tar failed... deleting failed .tar.xz and aborting."
                    rm $month_dir/$bin.tar.xz

                    exit 1
                fi

            else
                echo "$bin is a tarball"
            fi
        done
    done
done

