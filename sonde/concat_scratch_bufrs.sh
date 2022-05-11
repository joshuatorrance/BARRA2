#!/bin/bash

# Parameters
START_YEAR=2007
END_YEAR=2021

DIR=/scratch/hd50/jt4085/sonde/data-bufr-bins

OUTFILE_NAME=concat_sonde.bufr
TEMP_SUFFIX=.temp
    # Temp suffix cannot be ".bufr"

# Script
for y_dir in $DIR/*; do
    y=`basename $y_dir`
    echo $y

    if [[ $START_YEAR -gt $y ]]; then
        echo -e "\tBefore start year ($START_YEAR)"
        continue
    fi

    if [[ $END_YEAR -lt $y ]]; then
        echo -e "\tAfter end year ($END_YEAR)"
        continue
    fi

    if [ $1 -ne $y ]; then
        echo -e "\tYear not $1"
        continue
    fi

    for m_dir in $y_dir/*; do
        m=`basename $m_dir`
        echo -e "\t$m"

        for dt_dir in $m_dir/*; do
            dt=`basename $dt_dir`
            echo -e "\t\t$dt"

            outfile=$dt_dir/$OUTFILE_NAME
            temp_outfile=$outfile$TEMP_SUFFIX

            # If the output file exists this dir has already been done.
            if [ -e $outfile ]; then
                echo -e "\t\t\tConcat file already exists."
                continue
            fi

            # Append all the bufrs to the temp out file.
            cat $dt_dir/*.bufr > $temp_outfile

            # Delete all the *.bufrs
            rm $dt_dir/*.bufr

            # Move the temp out to the final file
            mv $temp_outfile $outfile
        done
    done
done


