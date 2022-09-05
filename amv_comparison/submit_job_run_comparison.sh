#!/bin/bash

#PBS -P hd50
#PBS -l ncpus=4
#PBS -l mem=16gb
#PBS -l walltime=12:00:00
#PBS -l storage=gdata/hd50+scratch/hd50+gdata/access+gdata/ig2
#PBS -l wd

# Load modules
module -t use /g/data/access/projects/access/modules
module load python3/3.8.5
module load eccodes3

# Run script
python3 /g/data/hd50/jt4085/BARRA2/amv_comparison/amv_comparison.py

