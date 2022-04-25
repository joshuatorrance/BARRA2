#!/bin/bash

# This job iterates through a directory of bufr files then passes
# each one to submit_job_organise_bufrs.sh

job_script=/g/data/hd50/jt4085/BARRA2/sonde/submit_job_organise_bufr.sh

# This script gets a lift of file with stations in the BARRA region
file_list_script=/g/data/hd50/jt4085/BARRA2/sonde/get_list_of_barra_stations.sh

bufr_file_dir=/scratch/hd50/jt4085/sonde/data-bufr
bufr_file_str="$bufr_file_dir/*.bufr"

NUMBER_OF_JOBS=100

# Calculate the batch size
# No ceil in bash so get python to do it.
num_files=`ls $bufr_file_str | wc -l`
batch_size=`python3 -c "from math import ceil; print(ceil($num_files / $NUMBER_OF_JOBS))"`

batch_size=1

for start_year in 2007 2011 2015 2019; do
    let end_year=$start_year+4

    # Use xargs to split the files in the dir into NUMBER_OF_JOBS sets
    # Use xargs to pass each set to qsub
    # Use shuf to shuffle the files to avoid blocking goegraphically similar files
    #ls $bufr_file_str \
    $file_list_script \
    | shuf \
    | xargs -n $batch_size \
    | xargs -I {} \
        qsub -v files_list="{}",start_year=$start_year,end_year=$end_year \
            $job_script
done

echo "Job submission script finished"

