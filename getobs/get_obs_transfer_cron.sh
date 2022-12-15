#!/bin/bash

###
# This script is intended for use with the BARRA2 project to deliver
# obs extracted with GetObs from nwp-verification-dev to Gadi.
#
# This script assumes that ssh keys or similar are such that
# passwords are not required to ssh to the input or output hosts.
#
# Run this script with a line in the crontab similar to:
# 10 0,6,12,18 * * * /path/to/script/get_obs_transfer_cron.sh
# add
# MAILTO=email@address.com
# to the start of the cron tab to have uncaptured stderr/stdout
# emailed to the user.
#
# joshua.torrance@bom.gov.au
###

## PARAMETERS
# Input
INPUT_USER="jtorranc"
INPUT_URL="nwp-verification-dev"
INPUT_DIR="/data/nwpv/barra2_test_getobs_jtorranc/GLB"

INPUT_HOST="$INPUT_USER@$INPUT_URL"
INPUT_LOCATION="$INPUT_HOST:$INPUT_DIR"

# Output
OUTPUT_USER="jt4085"
OUTPUT_URL="gadi.nci.org.au"
OUTPUT_DIR="/g/data/hd50/barra2/data/obs/production"

OUTPUT_HOST="$OUTPUT_USER@$OUTPUT_URL"
# Build the output loc later
#OUTPUT_LOCATION="$OUTPUT_HOST:$OUTPUT_DIR"

# Done files
DONE_EXTRACT="done.extract"
DONE_COPY="done.copy"


## SCRIPT
# Exit if any command fails
set -e

echo "Script started at `date`"

# Check input/output directories, ssh command will return
#  0 if the directory exists
#  1 if the directory doesn't exist
#  other codes (i.e. 255) if unable to connect

# Check the input directory exists
ssh $INPUT_HOST "[ -d $INPUT_DIR ]"
ret=$?
if [ $ret == 1  ]; then
    echo "Input directory, $INPUT_DIR, not found on $INPUT_HOST"
    echo "Exiting at `date`"
    exit 1
elif [ $ret != 0 ]; then
    echo "Unable to connect to $INPUT_HOST"
    echo "Exiting at `date`"
    exit 1
fi

# Check the output directory exists
ssh $OUTPUT_HOST "[ -d $OUTPUT_DIR ]"
ret=$?
if [ $ret == 1 ]; then
    echo "Output directory, $OUTPUT_DIR, not found on $OUTPUT_HOST"
    echo "Exiting at `date`"
    exit 1
elif [ $ret != 0 ]; then
    echo "Unable to connect to $OUTPUT_HOST"
    echo "Exiting at `date`"
    exit 1
fi

# Get a list of cycles
cycles=`ssh $INPUT_HOST ls $INPUT_DIR`

# For each cycle...
for cycle in $cycles; do
    echo $cycle

    # Cycle should be YYYYMMDDThhmmZ
    year=${cycle:0:4}
    month=${cycle:4:2}

    output_dir=$OUTPUT_DIR/$year/$month

    # Check if this cycle has already been transferred
    out_done_path=$output_dir/$cycle/$DONE_COPY
    if ssh $OUTPUT_HOST "test -e $out_done_path"; then
        echo -e "\tCycle already copied to destination, skipping copying"
    else
        # Check if the GetObs extraction is finished
        done_path=$INPUT_DIR/$cycle/$DONE_EXTRACT
        if ssh $INPUT_HOST "test -e $done_path"; then
            echo -e "\tCycle extraction complete."

            # Build the paths
            in=$INPUT_LOCATION/$cycle
            out=$OUTPUT_HOST:$output_dir

            echo -e "\tAttempting to copy"
            echo -e "\t$in"
            echo -e "\tto"
            echo -e "\t$out"

            # Create the destination directory
            ssh $OUTPUT_HOST "mkdir -p $output_dir"

            # Copy the files
            scp -3 -r $in $out

            # Check scp's return code
            if [ $? -eq 0 ]; then
                echo -e "\tCopy successful."
            else
                # Probably redundant given set -e above.
                echo -e "\tCopy failed."
                echo -e "\tExiting"

                exit 1
            fi

            # Create .done files
            ssh $OUTPUT_HOST "touch $output_dir/$cycle/$DONE_COPY"
            ssh $INPUT_HOST "touch $INPUT_DIR/$cycle/$DONE_COPY"
        else
            echo -e "\tCycle extraction not finished yet."
        fi
    fi

    # If the cycle has been copied then it can be deleted.
    done_path=$INPUT_DIR/$cycle/$DONE_COPY
    if ssh $INPUT_HOST "test -e $done_path"; then
        echo -e "\tDeleting cycle from input host."
        ssh $INPUT_HOST "rm -r $INPUT_DIR/$cycle"
    fi
done

echo "Script finished at `date`"
