#!/bin/bash

#PBS -P dp9
#PBS -l ncpus=1
#PBS -l mem=10gb
#PBS -l walltime=24:00:00
#PBS -l storage=gdata/hd50+scratch/hd50+gdata/access
#PBS -l wd

# Print out commands and exit if an error is encountered
set -ex

# Load modules
module load python3/3.8.5
module load eccodes3

# Parameters
script_path=/g/data/hd50/jt4085/BARRA2/sonde/organise_bufr.py
output_dir=/scratch/hd50/jt4085/sonde/data-bufr-bins

# Run script for each file path supplied
echo "files list:"
for f in $files_list; do
    echo -e "\t$f"

    # Make a temporary directory to work in.
    # Grab the first 11 characters of the name which happen
    #   to be the station ID
    temp_dir="temp/$(basename $f | head -c11)"

    echo "Creating temporary directory: $temp_dir"
    mkdir -p $temp_dir

    # Copy the file into the temp dir
    cp $f $temp_dir

    # cd into the temp dir so the scripts working files go there
    cd $temp_dir

    # Now run the script
    #  We're in the temporary dir so . as $temp_dir is not an absolute path
    python3 $script_path . $output_dir

    # Return to the previous directory
    echo "Returning to starting directory."
    cd -

    # Delete the temporary directory
    echo "Deleting temporary directory."
    rm -r $temp_dir
done

echo "Script finished at $(date)"

