#!/bin/bash

# This script uses a python script by Chun-Hsu to split a given bufr file
# into the usual 6 hour bins.
#
# I have yoinked a copy of Chun-Hsu's script so that I can tweak a
# detail or two.

script_dir=/g/data/hd50/jt4085/BARRA2/sonde/my_converter
script_path=$script_dir/organise_bufr.py

bufr_dir=/scratch/hd50/jt4085/sonde/data-bufr-test

archive_dir=/scratch/hd50/jt4085/sonde/data-bufr-bins

cd $script_dir
python3 $script_path $bufr_dir $archive_dir
