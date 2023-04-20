
## Parameters
SRC_DIR="barra2/barra-r2/prod/jt4085/cg406_2007.r1"
DEST_DIR="/scratch/hd50/jt4085/tarballs_from_tape"

# Date format for `date`
DATE_FMT='+%Y%m%dT%H%M'

START_CYCLE=20070901T0000
END_CYCLE=20080901T0000

ENS_MEMBERS="000 001 002 003 004 005 006 007 008 009 ctl"
STREAMS="MDL10M MDL1H MDL3H MDL6H PRS1H PRS3H SLV10M SLV1H SLV3H"


## Script
set -e
tabs 2

echo "Script started at `date`"

cycle=$START_CYCLE

while [ "$cycle" != "$END_CYCLE" ]
do
    echo $cycle

    year=`date '+%Y' -d $cycle`
    month=`date '+%m' -d $cycle`

    for ens in $ENS_MEMBERS; do
        echo -e "\t$ens"

        # Build the destination directory
        # Note the Z added to the cycle string
        # Add nc to match the normal barra directory structure
        dest_dir="$DEST_DIR/$year/$month/${cycle}Z/$ens/nc"

        # Build the source directory
        src_dir="$SRC_DIR/$year/$month/${cycle}Z/$ens/nc"

        # Create the dest directory
        echo -e "\t\tCreating destination directory"
        mkdir -p $dest_dir

        for stream in $STREAMS; do
            echo -e "\t\t$stream"

            # Check if this tarball has already been retrieved
            done_file_path="$dest_dir/$stream.done"
            echo -e "\t\t\tChecking if already retrieved"
            if [ -f $done_file_path ]; then
                echo -e "\t\t\t\tFile already retrieved"
                continue
            fi

            # Get the tarball from MDSS
            src_path="$src_dir/$stream.tar.gz"
            echo -e "\t\t\tGetting tarball from MDSS"
            mdss -P hd50 get $src_path $dest_dir

            # Create the .done file
            echo -e "\t\t\tCreating $stream.done file"
            touch $done_file_path
        done
    done

    # Next cycle
    # FIXME: why is date off by 3 hours?
    cycle=$(date $DATE_FMT -d "$cycle - 3 hours + 6 hours")
done

echo "Script finished at `date`"
