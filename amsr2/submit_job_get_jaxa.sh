#!/bin/bash

#PBS -P hd50
#PBS -l ncpus=1
#PBS -l mem=10gb
#PBS -q copyq
#PBS -l walltime=10:00:00
#PBS -l storage=gdata/hd50+scratch/hd50+gdata/access
#PBS -l wd

# Load modules

# Run script
/g/data/hd50/jt4085/BARRA2/amsr2/get_jaxa_files.py \
    -o /g/data/hd50/barra2/data/obs/amsr2 \
    -s 20130101T0000 \
    -e 20130101T0600

