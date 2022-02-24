#!/bin/sh

DATA_DIR="data"

for year in `ls $DATA_DIR`
do
    if [ $year -eq "2018" ]
    then
        continue
    fi

    year_dir=$DATA_DIR/$year

    for month in `ls $year_dir`
    do
        month_dir=$year_dir/$month

        for bin in `ls $month_dir`
        do
            if [[ $bin != *".tar.xz" ]]
            then
                bin_dir=$month_dir/$bin

                echo $bin_dir
                echo "  Compressing..."

                # Jump into the bin directory to ensure a clean tarball
                current_working_dir=`pwd`
                cd $bin_dir
                tar -cvJf $month_dir/$bin.tar.xz *.bufr
                cd $current_working_dir

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

