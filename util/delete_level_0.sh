#!/bin/bash

## PARAMETERS
ROOT_DIR=/g/data/hd50/barra2/data/prod
TRASH_ROOT_DIR=$ROOT_DIR/scripts/trash_to_delete

STREAMS="MDL10M MDL1H PRS1H PRS3H SLV10M SLV1H SLV3H"

# Ensemble names
# Use "deterministic" to add nothing to the path
# Use the ensemble names to add those to the path
#ENSEMBLES="deterministic"
ENSEMBLES="000 001 002 003 004 005 006 007 008 009 ctl"

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

# First Cycle - skip any cycle before this one
# Leave blank or commented to disable
FIRST_CYCLE="20070831T1800Z"

# Last Cycle - skip any cycle after this one
# Leave blank if not needed
LAST_CYCLE="20080831T1800Z"


## FUNCTIONS
# Explained in detail here:
# https://dev.to/meleu/how-to-join-array-elements-in-a-bash-script-303a
function join_by {
    local delim=${1-} first_arg=${2-}
    if shift 2; then
        printf %s "$first_arg" "${@/#/$delim}"
    fi
}


## SCRIPT
# Exit on any failure
set -e

echo -e "Script started at `date`\n"

# Set the tab width, default is very long
tabs 2

# Check the command line args
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

            if [[ "$FIRST_CYCLE" != "" && "$cycle" < "$FIRST_CYCLE" ]]; then
                echo -e "\t\t\t\tBefore FIRST_CYCLE, skipping"
                continue
            fi

            if [[ "$LAST_CYCLE" != "" && "$cycle" > "$LAST_CYCLE" ]]; then
                echo -e "\t\t\t\tAfter LAST_CYCLE, skipping"
                continue
            fi

            for ens_name in $ENSEMBLES; do
                echo -e "\t\t\t\t$ens_name"

                # If the trash directory is empty or doesn't exist then
                # files haven't been moved yet.
                trash_loc=$TRASH_ROOT_DIR/$suite_user/$suite_name/$year/$month/$cycle

                if [[ "$ens_name" == "deterministic" ]]; then
                    # Add nothing to the cycle path
                    ens_dir=$cycle_dir
                else
                    # Add the ensemble name to the cycle and trash path
                    ens_dir="$cycle_dir/$ens_name"
                    trash_loc="$trash_loc/$ens_name"
                fi

                # Add "nc" to the cycle path
                ens_dir="$ens_dir/nc"

                # Create the dir to unpack the 'to delete' files to
                mkdir -p $trash_loc

                # Iterate over each stream, getting the file to delete from the dictionary
                for stream in $STREAMS; do
                    echo -e "\t\t\t\t\t$stream"

                    tarball_path=$ens_dir/$stream.tar
                    if [ -f $tarball_path ]; then
                        stream_files_to_delete=${files_to_delete[$stream]}
                        if [[ "ALL" == "$stream_files_to_delete" ]]; then
                            # Move the entire tarball to trash
                            echo -e "\t\t\t\t\t\tMoving entire tarball to trash"
                            mv $tarball_path $trash_loc
                        else
                            # Build the list of internal paths to use with tar
                            # Add the -barra_r*.nc to ensure ta10 doesn't match ta1000 etc.
                            prefix="nc/$stream/"
                            suffix="-barra_r*.nc"

                            middle_of_list=`join_by "$suffix $prefix" $stream_files_to_delete`
                            stream_files_for_tar="$prefix$middle_of_list$suffix"

                            # Check if any of the files are in the tarball
                            # Turn the list of files into regex by replacing " "
                            #   Add -barra-r to avoid collitions (i.e. ta10 matching ta100)
                            #   Add | for regex OR
                            regex=`echo $stream_files_to_delete | tr ' ' "-barra_r|"`
                            regex=${stream_files_to_delete// /-barra_r|}

                            # Get the contents of the tarball
                            output=`tar --verbose --list --file=$tarball_path`

                            if [[ $output =~ $regex ]]; then
                                # If any of the files are present then extract
                                # them to the trash
                                # tar throws an error if some of the files are missing so suppress set -e
                                echo -e "\t\t\t\t\t\t\tExtracting files from tarball"
                                tar --extract \
                                    --file=$tarball_path \
                                    -C $trash_loc  \
                                    --wildcards $stream_files_for_tar

                                # Delete the files from the tarball
                                echo -e "\t\t\t\t\t\t\tDeleting files from tarball"
                                tar --delete \
                                    --file=$tarball_path \
                                    --wildcards $stream_files_for_tar
                            else
                                echo -e "\t\t\t\t\t\tNo relevant files found in tarball."
                            fi
                        fi
                    else
                        # Check the tarball is in the trash
                        if [ -f $trash_loc/$stream.tar ]; then
                            echo -e "\t\t\t\t\t\tTarball already moved to trash"
                        else
                            echo -e "\t\t\t\t\t\tTarball not found in trash or source dir"
                            echo -e "\t\t\t\t\t\t\tContinuing since there is nothing to do"
                            continue
                        fi
                    fi
                done
            done
        done
    done
done

echo -e "\nScript finished at `date`"
