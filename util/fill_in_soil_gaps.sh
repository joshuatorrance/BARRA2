#!/bin/bash

## Parameters
proj=hd50
year=2018
month=02
user=*
ens_to_check="000"
ens_to_check="000 001 002 003 004 005 006 007 008 009 ctl"
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

            # Construct a list of files to extract
            declare -a action_list=()
            for var in $vars_to_check; do
                echo -e "\t\t\t$var"

                regex="${var}-barra"
                if [[ -n `tar -tvf $tarball_path | grep ${regex}` ]]; then
                    echo -e "\t\t\t\tFile found in tarball"
                else
                    echo -e "\t\t\t\tFile not found in tarball"

                    action_list+=("${regex}*.nc")
                fi
            done

            echo

            if [[ ${#action_list[@]} > 0 ]]; then
                echo -e "\t\t\tFetching tarball from MDSS"
                mdss_path=barra2/barra-r2/prod/$user/cg406*/$year/$month/$cycle/$ens/nc/$stream.tar.gz
                mdss -P $proj get $mdss_path $temp_file_dir

                echo -e "\t\t\tUnpacking files from tarball"
                new_tarball=$temp_file_dir/${stream}.tar.gz

                # Clear out the extraction dest first
                # Any existing files will make a mess with wildcards below
                rm -rf $temp_file_dir/nc/$stream

                # Create the directory structure for the old tar
                mkdir -p $temp_file_dir/nc/$stream

                # Extract the files
                tar -zx \
                    --file=$new_tarball \
                    --directory=$temp_file_dir/nc/$stream \
                    --wildcards \
                    ${action_list[*]}

                # Add nc/$stream/ infront of each file name
                # Use a subshell and cd to handle wildcards
                echo -e "\t\t\tAdding files to old tarball"
                (
                 cd $temp_file_dir &&
                 tar --append \
                     --file=$tarball_path \
                     --wildcards \
                     ${action_list[*]/#/nc/$stream/}
                )

                # Remove the tarball just in case
                rm $new_tarball
            else
                echo -e "\t\t\tNothing to do"
            fi
        done
    done
done

rm -r $temp_file_dir

echo -e "\nScript finished at `date`"
