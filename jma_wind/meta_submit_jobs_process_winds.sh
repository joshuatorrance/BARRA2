#!/bin/bash

## PARAMETERS
START_DATETIME_STR='2005-06-01T00:00+0000'
END_DATETIME_STR='2015-08-01T00:00+0000'

# Size of the bins - 6 hours
let BIN_SIZE_SEC=6*60*60
# Offset of the bins relative to midnight
# i.e. bin goes from 9pm to 3am so offset is 3 hours
let BIN_OFFSET_SEC=$BIN_SIZE_SEC/2

# Job script
SCRIPT=/g/data/hd50/jt4085/BARRA2/jma_wind/submit_job_process_winds.sh

# Number of jobs to submit
N_JOBS=100

## SCRIPT
# Convert the datetime strings to timestamps
start_timestamp=`date --date=$START_DATETIME_STR '+%s'`
end_timestamp=`date --date=$END_DATETIME_STR '+%s'`

# Align the start and end to the bin edges
function align_to_bin_edge {
    # $1 is -s or -e to align to the  start or end of the bin
    # $2 is the timestamp (i.e. seconds since midnight on 1 Jan 1970)
    if [ $1 = "-s" ]; then
        local is_end=0
    elif [ $1 = "-e" ]; then
        local is_end=1
    else
        echo "ERROR in align_to_bin_edge"
        exit 1
    fi

    # This calculate the start of the bin.
    # If we're after the end just add a bin's width
    local aligned=$(( $2 - ( ( $2 + $BIN_OFFSET_SEC ) % $BIN_SIZE_SEC ) + $is_end * $BIN_SIZE_SEC ))

    echo $aligned
    return 0
}

align_start_timestamp=`align_to_bin_edge -s $start_timestamp`
align_end_timestamp=`align_to_bin_edge -e $end_timestamp`

# Calculate the number of bins in the range
n_bins=$(( ($align_end_timestamp-$align_start_timestamp)/$BIN_SIZE_SEC ))

# Calculate how many bins per job
# Little trick here to do ceil
n_bins_per_job=$(( ($n_bins+$N_JOBS-1)/$N_JOBS ))

# Iterate over the range and submit the jobs.
for i in $(seq 0 $(($N_JOBS-1))); do
    echo "Job $i of $N_JOBS"
    start=$(( align_start_timestamp + $i*$n_bins_per_job*$BIN_SIZE_SEC ))
    end=$(( $start + $n_bins_per_job*$BIN_SIZE_SEC ))

    # Start should not be after align_end_timestamp
    if [[ $start -ge $align_end_timestamp ]]; then
        echo "Chunk start outside of time range."
        continue
    fi

    # End should not be after align_end_timestamp
    end=$(( $end < $align_end_timestamp ? $end : $align_end_timestamp ))

    echo -e "Start:\t$(date --utc --date="@$start" '+%Y%m%dT%H%M%z')"
    echo -e "End:\t$(date --utc --date="@$end" '+%Y%m%dT%H%M%z')"

    # Submit job
    qsub -v start_timestamp="$start",end_timestamp="$end",bin_size_sec="$BIN_SIZE_SEC" \
        $SCRIPT

    echo
done

echo "Meta script finished at $(date)"
