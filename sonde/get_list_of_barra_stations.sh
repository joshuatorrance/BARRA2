#!/bin/bash

bufr_file_dir=/scratch/hd50/jt4085/sonde/data-bufr
bufr_file_str="$bufr_file_dir/*.bufr"

station_list="/g/data/hd50/barra2/data/obs/igra/doc/igra2-station-list.txt"

# BARRA2 region:
EAST=210
EAST=-150
WEST=90
NORTH=15
SOUTH=-60

files=`ls $bufr_file_str`

for f in $files; do
    filename=`basename $f`
    station_name=${filename:0:11}

    line=`grep $station_name $station_list`

    lat=${line:12:8}
    lon=${line:21:9}

    #echo $f
    #echo -e "\t$lat"
    #echo -e "\t$lon"

    # Got to use float arithmetic hence piping to bc below
    if [[ $lat == "-98.8888" || $lon == "-998.8888" ]]; then
        # Mobile station
        echo $f
        continue
    elif (( $(echo "$lat > $SOUTH" | bc -l) )) && \
         (( $(echo "$lat < $NORTH" | bc -l) )); then
        # Inside N/S
        if (( $(echo "$WEST < $EAST" | bc -l) )); then
            # E/W does not loop around
            if (( $(echo "$lon >  $WEST" | bc -l) )) && \
               (( $(echo "$lon <  $EAST" | bc -l) )); then
                # Inside E/W
                # Inside BARRA region
                echo $f
                #echo -e "\tInside"
                #continue
            fi
        else
            # E/W loops around
            if (( $(echo "$lon >  $WEST" | bc -l) )) || \
               (( $(echo "$lon <  $EAST" | bc -l) )); then
                # Inside E/W
                # Inside BARRA region
                echo $f
                #echo -e "\tInside"
                #continue
            fi
        fi
    fi

    # Outside BARRA region
#    echo -e "\tOutside"
done

