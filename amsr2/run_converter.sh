#!/bin/bash

usage() {
    echo "run_converter.sh [-h] [-i infile] [-o outfile]"
    echo "    -i input file path"
    echo "    -o output file path"
    echo "    -h display this message and exit"
    exit
}

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


# Path to binary (I have some env vars set)
CONVERT_BIN="$CONVERT_AMSR2_DIR/convert_amsr2/src/convert_amsr2.exe"

# Run the converter
$CONVERT_BIN $input $output

