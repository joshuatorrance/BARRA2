#!/bin/bash

## PARAMETERS
MTSAT1_DIR=/scratch/hd50/jt4085/jma_wind/MTSAT-1R
MTSAT2_DIR=/scratch/hd50/jt4085/jma_wind/MTSAT-2

## SCRIPT
echo "SatelliteName,Datetime,Hemisphere"
for sat_dir in $MTSAT1_DIR $MTSAT2_DIR; do
    sat=`basename $sat_dir`

    for year_mon_dir in $sat_dir/*/; do
        year_mon=`basename $year_mon_dir`
        year=${year_mon:0:4}
        mon=${year_mon:4:2}

        for day_dir in $year_mon_dir*/; do
            day=`basename $day_dir`

            for hour_dir in $day_dir*/; do
                hour=`basename $hour_dir`

                dt="${year}${mon}${day}T${hour}00Z"

                for hemi_dir in $hour_dir*/; do
                    hemi=`basename $hemi_dir`
                    if [ "$hemi" = "f" ]; then
                        hemi=n
                    fi

                    echo $sat,$dt,$hemi
                done
            done
        done
    done
done

