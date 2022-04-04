#!/bin/bash

# This job iterates through a directory of bufr files then passes
# each one to submit_job_organise_bufrs.sh

job_script=/g/data/hd50/jt4085/BARRA2/sonde/my_converter/submit_job_organise_bufr.sh

bufr_file_dir=/scratch/hd50/jt4085/sonde/data-bufr
bufr_file_str="$bufr_file_dir/*.bufr"

NUMBER_OF_JOBS=100

# Calculate the batch size
# No ceil in bash so get python to do it.
num_files=`ls $bufr_file_str | wc -l`
batch_size=`python3 -c "from math import ceil; print(ceil($num_files / $NUMBER_OF_JOBS))"`

# Use xargs to split the files in the dir into NUMBER_OF_JOBS sets
# Use xargs to pass each set to qsub
ls $bufr_file_str \
| xargs -n $batch_size \
| xargs -I {} \
    qsub -v files_list="{}" $job_script

echo "Job submission script finished"

