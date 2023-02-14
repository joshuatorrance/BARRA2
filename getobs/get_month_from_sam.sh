#!/bin/bash

## PARAMETERS
SRC_HOST=jtorranc@sam.bom.gov.au
DST_HOST=jt4085@gadi.nci.org.au

SRC_DIR=/samaccess_prod/ACCESS_prod/access_g3_getobs
DST_DIR=/scratch/hd50/jt4085/get_obs/g3_from_sam


## SCRIPTS
echo "Script started at `date`"

# Get the year and month from the command line
if [[ $# != 2 ]]; then
    echo "Two command line args expected, got $#"
    echo "Usage: ./get_month_from_sam.sh [4 digit year] [2 digit month]"
    exit 1
fi

year=$1
month=$2

# Check the length of year and month and whether they're integers
if [[ $year =~ ^[0-9]{4}$ ]]; then
    echo "Year: $year"
else
    echo "Year doesn't seem to the valid: $year"
    echo "Usage: ./get_month_from_sam.sh [4 digit year] [2 digit month]"
    exit 1
fi

if [[ $month =~ ^[0-9]{2}$ && 1 -le $month  && $month -le 12 ]]; then
    echo "Month: $month"
else
    echo "Month doesn't seem to the valid: $month"
    echo "Usage: ./get_month_from_sam.sh [4 digit year] [2 digit month]"
    exit 1
fi

# Build the source and dest paths
src_month_dir=$SRC_DIR/$year/$month
dst_month_dir=$DST_DIR/$year/$month

# Get the list of cycles from the source
cycle_list=`ssh $SRC_HOST ls $src_month_dir`
ret=$?
if [[ $ret != 0 ]]; then
    echo "Failed to get cycles from $SRC_HOST at $src_month_dir"
    exit 1
fi


# Iterate through the cycles and send them to the destination
for cycle in $cycle_list; do
    echo $cycle

    # Build the source tarball's path
    src_cycle_dir=$src_month_dir/$cycle
    src_tarball_path=$src_cycle_dir/${cycle}_obs.tar.gz

    # Build the dest paths
    dst_cycle_dir=$dst_month_dir/$cycle
    dst_tarball_path=$dst_cycle_dir/${cycle}_obs.tar.gz

    # Check if the tarball exists at the destination
    echo -e "\tChecking if dest tarball already exists"
    ssh $DST_HOST "[ -f $dst_tarball_path ]"
    ret=$?
    if [[ $ret == 0 ]]; then
        echo -e "\tTarball already exists at dest, moving on"
        continue
    fi

    # Check the source tarball exists
    echo -e "\tChecking source tarball exists"
    ssh $SRC_HOST "[ -f $src_tarball_path ]"
    ret=$?
    if [[ $ret != 0 ]]; then
        echo "Failed to find $src_tarball_path on $SRC_HOST"
        exit 1
    fi

    # Create the destination directory
    echo -e "\tCreating destination directory"
    ssh $DST_HOST mkdir -p $dst_cycle_dir
    if [[ $? != 0 ]]; then
        echo "Something went wrong with mkdir for $dst_cycle_dir on $DST_HOST"
        exit 0
    fi

    # Copy the file to the destination
    echo -e "\tCopying file to destination"
    scp -3 $SRC_HOST:$src_tarball_path $DST_HOST:$dst_cycle_dir
    if [[ $? != 0 ]]; then
        echo "Something went wrong with scp"
        exit 0
    fi

    # TODO: check the size at src and dst? md5 hash?
done

echo "Script finished at `date`"

