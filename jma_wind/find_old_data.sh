#!/bin/bash

# This script looks for symlinks called "JMAWINDS_*.bufr" and their targets.
# If found these should be deleted as their remants of the old data from
# the archive, not the newly reprocessed data from JMA.

PROD_DIR=/g/data/hd50/barra2/data/obs/production
ARCHIVE_DIR=/scratch/hd50/jt4085/jma_wind/old_production_bufrs/

for y in {2015..2016}; do
    echo $y

    y_dir=$PROD_DIR/$y

    for m in {01..12}; do
        m_dir=$PROD_DIR/$y/$m
        echo -e "\t$m"

        for dt_dir in $m_dir/*/; do
            dt=`basename $dt_dir`
            echo -e "\t\t$dt"

            archive_dt_dir=$ARCHIVE_DIR$y/$m/$dt

            link_list=`find $dt_dir -name JMAWINDS_*.bufr -type l`

            for f in $link_list; do
                target_f=`readlink -f $f`

                # Archive files by uncommenting the rm below once tested.
                echo -e "\t\t\tArchiving file "`basename $target_f`
                mkdir -p $archive_dt_dir
                mv $target_f $archive_dt_dir

                echo -e "\t\t\tDeleting link "`basename $f`
                rm $f

                echo
            done
        done
    done
done

echo "Script finished at $(date)"
