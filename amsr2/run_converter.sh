#!/bin/bash

usage() {
    echo "run_converter.sh [-h] [-i infile] [-o outfile]"
    echo "    -i input file path"
    echo "    -o output file path"
    echo "    -h display this message and exit"
    exit
}

# Converter Paths
CONVERT_AMSR2_DIR=/g/data/hd50/jt4085/convert_amsr2/build-amsr2-hdf5-to-bufr-master/install
CONVERT_BIN=$CONVERT_AMSR2_DIR/convert_amsr2/src/convert_amsr2.exe

# Required Library paths
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CONVERT_AMSR2_DIR/hdf5/intel/1.8.20/lib:$CONVERT_AMSR2_DIR/eccodes/intel/2.6.0/lib

# Parse arguments
while getopts i:o:h flag
do
  case "${flag}" in
    i) input=${OPTARG};;
    o) output=${OPTARG};;
    h) usage;
  esac
done

if [ ! $input ] || [ ! $output ]
then
  echo "Input or output parameters missing."
  usage
fi


# Run the converter
$CONVERT_BIN $input $output

