#!/bin/bash

## PARAMETERS
ROOT_DIR=/g/data/hd50/barra2/data/prod
USERS="as2291 chs548 jt4085 sjr548"
TRASH_ROOT_DIR=$ROOT_DIR/trash_to_delete

START_YEAR=2007
END_YEAR=2020

STREAMS="MDL1H PRS1H PRS3H SLV10M SLV1H SLV3H"

# Files to delete - from /g/data/hd50/barra2/data/prod/scripts/delete_level0.list
declare -A files_to_delete=(
    ["MDL10M"]="ALL"  # MDL10M has already been deleted. Hence not in STREAMS
    ["MDL1H"]="ALL"
    ["PRS1H"]="ALL"
    ["PRS3H"]="ALL"
    ["SLV10M"]="ALL"
    ["SLV1H"]="cs_up_lw_radlev av_land_evap_pot av_cs_up_lw_flx max_temp_scrn sb_cin av_rate_ls_prcp soil_mois av_oswrad_flx av_rate_ls_snow av_rate_conv_snow av_rate_evap_canopy av_swsfcdown sfc_pres av_rate_evap_soil av_cs_up_sw_flx av_low_cld av_evap_sea av_rate_transpiration av_lat_hflx av_swirrtop av_mid_cld cs_dsfc_lw_flx min_temp_scrn av_netlwsfc av_netswsfc av_olr ttl_col_qcf soil_mois_frozen_frac av_lwsfcdown cs_dsfc_sw_flx ttl_col_q av_sens_hflx cs_usfc_sw_flx abl_ht av_prcp_rate av_ttl_cld ttl_col_qcl sb_cape av_hi_cld"
    ["SLV3H"]="soil_mois av_roughness_len_tiles av_roughness_len soil_temp soil_mois_frozen_frac tiles_snow_depth av_rate_ssfc_runoff av_rate_snowmelt seaice av_uwnd_strs av_rate_sfc_runoff snow_amt_lnd av_vwnd_strs"
)

# Last Cycle - skip any cycle after this one
LAST_CYCLE="20220630T1800Z"

## SCRIPT
# Exit on any failure
set -e

echo -e "Script started at `date`\n"

if [ "$#" -ne 2 ]; then
    echo "2 commandline arguments expected."
    echo -e "\t./delete_level_0.sh [suite_username] [suite_name]"
    echo -e "\tE.g.: ./delete_level_0.sh jt4085 cg406_2007.r1"

    exit 1
fi

# Grab the user and suite name from the command line
suite_user=$1
suite_name=$2

echo "Suite user: $suite_user"
echo "Suite name: $suite_name"

suite_dir=$ROOT_DIR/$suite_user/$suite_name

# Check the suite dir exists
if [ ! -d $suite_dir ]; then
    echo "Suite directory not found:"
    echo $suite_dir
    exit 1
fi

# Iterate through the suite's cycles
for year_dir in $suite_dir/20??; do
    year=`basename $year_dir`
    echo -e "\t$year"

    for month_dir in $year_dir/??; do
        month=`basename $month_dir`
        echo -e "\t\t$month"

        for cycle_dir in $month_dir/*; do
            cycle=`basename $cycle_dir`

            echo -e "\t\t\t$cycle"

            if [[ "$cycle" > "$LAST_CYCLE" ]]; then
                echo -e "\t\t\t\tAfter LAST_CYCLE, skipping"
                continue
            fi

            # Add "nc" to the cycle path since we're only interested in the deterministic files
            cycle_dir="$cycle_dir/nc"

            # If the trash directory is empty or doesn't exist then
            # files haven't been moved yet.
            trash_loc=$TRASH_ROOT_DIR/$suite_user/$suite_name/$year/$month/$cycle

            # Create the dir to unpack the 'to delete' files to
            mkdir -p $trash_loc

            # Iterate over each stream, getting the file to delete from the dictionary
            for stream in $STREAMS; do
                echo -e "\t\t\t\t$stream"

                tarball_path=$cycle_dir/$stream.tar
                if [ -f $tarball_path ]; then
                    stream_files_to_delete=${files_to_delete[$stream]}
                    if [[ "ALL" == "$stream_files_to_delete" ]]; then
                        # Move the entire tarball to trash
                        echo -e "\t\t\t\t\tMoving entire tarball to trash"
                        mv $tarball_path $trash_loc
                    else
                        # Unpack each file from the tarball
                        for file in $stream_files_to_delete; do
                            echo -e "\t\t\t\t\t$file"

                            # Add the -barra_r2 to ensure ta10 doesn't match ta1000 etc.
                            internal_path="nc/$stream/${file}-barra_r2*.nc"

                            # Check if the file is in the tarball
                            # Silence the output and check the return code
                            # Suppress set -e for this command
                            set +e
                            tar --list --file=$tarball_path --wildcards $internal_path \
                                > /dev/null 2>&1
                            ret=$?
                            set -e

                            if [ $ret -eq 0 ]; then
                                # File exists

                                # Extract the file from the tarball
                                echo -e "\t\t\t\t\t\tExtracting from tarball"
                                tar --extract --file=$tarball_path \
                                    -C $trash_loc  \
                                    --wildcards $internal_path

                                # Delete the file from the tarball
                                echo -e "\t\t\t\t\t\tDeleting from tarball"
                                tar --file=$tarball_path \
                                    --wildcards $internal_path \
                                    --delete
                            else
                                echo -e "\t\t\t\t\t\tNot in tarball"
                                
                                # Check the file is in the trash
                                if [ -f $trash_loc/$internal_path ]; then
                                    echo -e "\t\t\t\t\t\tFile in trash"
                                else
                                    echo -e "\t\t\t\t\t\tFile not found in trash!"
                                    echo -e "\t\t\t\t\t\tExiting since this shouldn't happen!"
                                    exit 1
                                fi
                            fi
                        done
                    fi
                else
                    # Check the tarball is in the trash
                    if [ -f $trash_loc/$stream.tar ]; then
                        echo -e "\t\t\t\t\tTarball already moved to trash"
                    else
                        echo -e "\t\t\t\t\tTarball not found in trash or source dir."
                        echo -e "\t\t\t\t\t\tExiting since this shouldn't happen!"
                        exit 1
                    fi
                fi
            done

            # TODO: Remove the following two lines
            echo "Finished testing on cycle: $cycle"
            exit 0
        done
    done
done

echo -e "\nScript finished at `date`"
