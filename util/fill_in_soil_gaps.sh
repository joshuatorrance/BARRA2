#!/bin/bash

## Parameters
proj=hd50
year=2010
month=07
user=jt4085
ens_to_check="003"
steams_to_check="SLV1H"
vars_to_check="soil_mois soil_mois_frozen_frac"

root_dir=/g/data/$proj/barra2/data/prod/$user/cg406_*/$year/$month
temp_file_dir=/scratch/$proj/jt4085/tmp/fixing_soil_gaps


## Script
set -e

echo -e "Script started at `date`\n"

mkdir -p $temp_file_dir

for cycle_dir in $root_dir/*; do
    cycle=`basename $cycle_dir`

    echo $cycle

    for ens in $ens_to_check; do
        echo -e "\t$ens"

        for stream in $steams_to_check; do
            echo -e "\t\t$stream"

            tarball_path=$cycle_dir/$ens/nc/$stream.tar

            if [ ! -f $tarball_path ]; then
                echo -e "\t\tTarball not found"
                echo -e "\t\t$tarball_path"

                exit 1
            fi

            for var in $vars_to_check; do
                echo -e "\t\t\t$var"

                regex="${var}-barra"
                if [[ -n `tar -tvf $tarball_path | grep ${regex}` ]]; then
                    echo -e "\t\t\t\tFile found in tarball"
                else
                    echo -e "\t\t\t\tFile not found in tarball"

                    echo -e "\t\t\t\tFetching tarball from MDSS"
                    mdss -P $proj get barra2/barra-r2/prod/$user/cg406*/$year/$month/$cycle/$ens/nc/$stream.tar.gz $temp_file_dir

                    echo -e "\t\t\t\tUnpacking file from tarball"
                    new_tarball=$temp_file_dir/${stream}.tar.gz

                    # Create the directory structure for the old tar
                    mkdir -p $temp_file_dir/nc/$stream

                    # Get the name of the file to extract
                    file_to_extract=`tar -tvf $new_tarball | \
                                     grep $regex | \
                                     awk '{print $NF}'`

                    # Extract the file
                    tar -zx --directory=$temp_file_dir/nc/$stream \
                        --file=$new_tarball $file_to_extract


                    echo -e "\t\t\t\tAdding file to old tarball"
                    tar --append --file=$tarball_path  --directory=$temp_file_dir nc/$stream/$(basename $file_to_extract)

                    # Remove the tarball just in case
                    rm $new_tarball

                fi
            done
        done

    done
done

rm -r $temp_file_dir

echo -e "\nScript finished at `date`"
