#!/bin/bash

#PBS -P hd50
#PBS -l ncpus=10
#PBS -l mem=20gb
#PBS -l walltime=10:00:00
#PBS -l storage=gdata/hd50+scratch/hd50+gdata/access
#PBS -l wd

# Load modules
module load python3/3.8.5
module load eccodes3

# Run script
# Threading in the script isn't working well
# So multi-run the script itself
echo "job number: $job_num"

python3 \
    /g/data/hd50/jt4085/BARRA2/sonde/run_conversion.py \
    $job_num &

# Wait for the threads to close
wait

