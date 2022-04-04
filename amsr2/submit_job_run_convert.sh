#!/bin/bash

#PBS -P hd50
#PBS -l ncpus=10
#PBS -l mem=5gb
#PBS -l walltime=24:00:00
#PBS -l storage=gdata/hd50+scratch/hd50+gdata/access
#PBS -l wd

# Load modules

# Run script
/g/data/hd50/jt4085/BARRA2/amsr2/run_conversion_on_dirs.sh 12

