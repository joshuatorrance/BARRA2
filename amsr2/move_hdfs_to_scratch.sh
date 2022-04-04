#!/bin/bash

# Root directories
GDATA_DIR=/g/data/hd50/barra2/data/obs/amsr2
SCRATCH_DIR=/scratch/hd50/jt4085/amsr2/hdf

# Year to move
YEAR=2012

# cd to gdata dir to make paths simpler
cd $GDATA_DIR

echo Year: $YEAR
for hdf_file in $YEAR/*/*/*.h5; do
    echo `basename $hdf_file`
    
    dest=$SCRATCH_DIR/$hdf_file

    mkdir -p `dirname $dest`

    mv $hdf_file $dest

    # Newline for readability
    #echo ""
done

echo "Script finished at: " `date`

