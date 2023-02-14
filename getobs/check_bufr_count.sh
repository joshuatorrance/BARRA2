#!/bin/bash

## PARAMETERS
ROOT_DIR=/g/data/hd50/barra2/data/obs/production
year=2023
month=02

## SCRIPT
echo "Script started at `date`"

for cycle_dir in $ROOT_DIR/$year/$month/*; do
    cycle=`basename $cycle_dir`
    bufr_count=`find $cycle_dir -type f -name *.bufr | wc -l`

    echo "$cycle : $bufr_count"
done

echo "Script finished at `date`"

