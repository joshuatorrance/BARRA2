#!/bin/bash

# This script simply submits a number of jobs with a passing the
# job number as an argument.

job_script=/g/data/hd50/jt4085/BARRA2/sonde/my_converter/submit_job_convert_sonde.sh

N=100

for i in $(seq 0 $N); do
    echo $i
    qsub -v job_num=$i $job_script
done

echo "Script finished"

