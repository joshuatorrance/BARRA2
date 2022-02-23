#!/bin/bash
# To use the filters on Gadi use something like:

module load eccodes
bufr_filter -o appended.bufr bufr_append.filter input_*.bufr 
