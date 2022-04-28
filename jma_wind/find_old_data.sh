#!/bin/bash

# This script looks for symlinks called "JMAWINDS_*.bufr" and their targets.
# If found these should be deleted as their remants of the old data from
# the archive, not the newly reprocessed data from JMA.

PROD_DIR=/g/data/hd50/barra2/data/obs/production

for y in {2005..2015}; do
    echo $y

    y_dir=$PROD_DIR/$y

    link_list=`find $y_dir -name JMAWINDS_*.bufr -type l | head -n 2`

    for f in $link_list; do
        target_f=`readlink -f $f`

        echo $f
        echo $target_f

        exit 0

        # TODO: Delete files by uncommenting the rm below once tested.
        # rm $f
        # rm $target_f

        # TODO: alternatively move them to the archive - need to extract datetime
    done

done
