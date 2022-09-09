#!/bin/bash

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
OUTPUT_DIR="/scratch/hd50/jt4085/get_obs"

OUTPUT_HOST="$OUTPUT_USER@$OUTPUT_URL"
OUTPUT_LOCATION="$OUTPUT_HOST:$OUTPUT_DIR"

# Done files
DONE_EXTRACT="done.extract"
DONE_COPY="done.copy"


## SCRIPT
# Exit if any command fails
set -e

echo "Script started at `date`"

# Check the input directory exists
if ssh $INPUT_HOST "test ! -e $INPUT_DIR"; then
    echo "Input directory, $INPUT_DIR, not found on $INPUT_HOST"
    echo "Exiting at `date`"
    exit 1
fi

# Check the output directory exists
if ssh $OUTPUT_HOST "test ! -e $out_done_path"; then
    echo "Output directory, $OUTPUT_DIR, not found on $OUTPUT_HOST"
    echo "Exiting at `date`"
    exit 1
fi


# Get a list of cycles
cycles=`ssh $INPUT_HOST ls $INPUT_DIR`

# For each cycle...
for cycle in $cycles; do
    echo $cycle

    # Check if this cycle has already been transferred
    out_done_path=$OUTPUT_DIR/$cycle/$DONE_COPY
    if ssh $OUTPUT_HOST "test -e $out_done_path"; then
        echo -e "\tCycle already copied to destination, skipping copying"
    else
        # Check if the GetObs extraction is finished
        done_path=$INPUT_DIR/$cycle/$DONE_EXTRACT
        if ssh $INPUT_HOST "test -e $done_path"; then
            echo -e "\tCycle extraction complete."

            # Copy files to the destination
            in=$INPUT_LOCATION/$cycle
            out=$OUTPUT_LOCATION
            echo -e "\tAttempting to copy"
            echo -e "\t$in"
            echo -e "\tto"
            echo -e "\t$out"

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

            # Create .done file
            ssh $OUTPUT_HOST "touch $OUTPUT_DIR/$cycle/$DONE_COPY"
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

