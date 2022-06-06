#!/bin/bash

#PBS -P hd50
#PBS -l ncpus=1
#PBS -l mem=5gb
#PBS -l walltime=24:00:00
#PBS -l storage=gdata/hd50+scratch/hd50+gdata/access
#PBS -l wd

# Load modules

# Run script
SCRIPT=/g/data/hd50/jt4085/BARRA2/amsr2/compress_bufrs.sh

$SCRIPT 2022

