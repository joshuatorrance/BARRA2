#!/bin/bash

## PARAMETERS
ROOT_DIR=/g/data/hd50/barra2/data/prod
TRASH_ROOT_DIR=$ROOT_DIR/scripts/trash_to_delete

STREAMS="MDL10M MDL1H PRS1H PRS3H SLV10M SLV1H SLV3H"

# Ensemble names
# Use "deterministic" to add nothing to the path
# Use the ensemble names to add those to the path
#ENSEMBLES="deterministic"
ENSEMBLES="000 001 002 003 004 005 006 007 008 009 ctl"

# Files to restore
# NONE - restore nothing
# ALL - restore all files in trash
# filename1 filename2 - restore files that match
# TODO: files from wholly deleted tarballs won't be restored yet
declare -A files_to_restore=(
    ["MDL10M"]="NONE"
    ["MDL1H"]="NONE"
    ["PRS1H"]="NONE"
    ["PRS3H"]="NONE"
    ["SLV10M"]="NONE"
    ["SLV1H"]="min_temp_scrn av_ttl_cld"
    ["SLV3H"]="ALL"
)

# First Cycle - skip any cycle before this one
# Leave blank or commented to disable
FIRST_CYCLE="20070831T1800Z"

# Last Cycle - skip any cycle after this one
# Leave blank if not needed
LAST_CYCLE="20080831T1800Z"


## FUNCTIONS
# Explained in detail here:
# https://dev.to/meleu/how-to-join-array-elements-in-a-bash-script-303a
# join_by ", " "a b c"
# => "a, b, c"
function join_by {
    local delim=${1-} first_arg=${2-}
    if shift 2; then
        printf %s "$first_arg" "${@/#/$delim}"
    fi
}


## SCRIPT
# Exit on any failure
set -e

echo -e "Script started at `date`\n"

# Set the tab width, default is very long
tabs 2

for user_dir in $TRASH_ROOT_DIR/*; do
  user=`basename $user_dir`
  echo $user

  for suite_dir in $user_dir/*; do
    if [ -f $suite_dir ]; then
      # suite_dir is not a dir, so move on
      continue
    fi

    suite=`basename $suite_dir`
    echo -e "\t$suite"

    for year_dir in $suite_dir/*; do
      year=`basename $year_dir`

      for month_dir in $year_dir/*; do
        month=`basename $month_dir`

        for cycle_dir in $month_dir/*; do
          cycle=`basename $cycle_dir`

          echo -e "\t\t$cycle"

          if [[ "$FIRST_CYCLE" != "" && "$cycle" < "$FIRST_CYCLE" ]]; then
            echo -e "\t\t\t\tBefore FIRST_CYCLE, skipping"
            continue
          fi

          if [[ "$LAST_CYCLE" != "" && "$cycle" > "$LAST_CYCLE" ]]; then
            echo -e "\t\t\t\tAfter LAST_CYCLE, skipping"
            continue
          fi

          for ens_name in $ENSEMBLES; do
            echo -e "\t\t\t$ens_name"

            restore_path=$ROOT_DIR/$user/$suite/$year/$month/$cycle

            if [[ "$ens_name" == "deterministic" ]]; then
              # Add nothing to the cycle path
              ens_dir=$cycle_dir
            else
              # Add the ensemble name to the paths
              ens_dir="$cycle_dir/$ens_name"
              restore_path="$restore_path/$ens_name"
            fi

            # Add "nc" to the cycle paths
            restore_path="$restore_path/nc"

            for stream in $STREAMS; do
              echo -e "\t\t\t\t$stream"
              stream_dir=$ens_dir/nc/$stream
              stream_files_to_restore=${files_to_restore[$stream]}

              # TODO: Restore files that have been "deteled" as a whole tarball
              # Currently only looks in unpacked nc/[stream]/*.nc dir

              if [[ "ALL" == "$stream_files_to_restore" ]]; then
                echo -e "\t\t\t\t\tRestoring all"

                # Use shorter relative path so internal path is correct
                filepaths_to_restore="nc/$stream/*-barra_r*.nc"
              elif [[ "NONE" == "$stream_files_to_restore" ]]; then
                echo -e "\t\t\t\t\tRestoring nothing"

                # Nothing to do so continue
                continue
              else
                echo -e "\t\t\t\t\tRestoring $stream_files_to_restore"

                # Use join-by to build a list of filepaths to restore
                # Use shorter relative path so internal path is correct
                prefix="nc/$stream/"
                suffix='-barra_r*.nc'
                delim="$suffix $prefix"
                middle_of_list=`join_by "$delim" $stream_files_to_restore`
                filepaths_to_restore="$prefix$middle_of_list$suffix"
              fi

              # Check there's a tarball to restore to
              restore_tarball_path="$restore_path/${stream}.tar"              
              if [ ! -f $restore_tarball_path ]; then
                echo -e "\t\t\t\t\tTarball doesn't exist yet, creating empty"
                
                # Create an empty tarball to add to later
                tar --create \
                  --file $restore_tarball_path \
                  --files-from /dev/null
              else
                echo -e "\t\t\t\t\tTarball already exists"
              fi

              # Add files into the tarball
              # Wildcards are tricker in this scenario
              # Use cd in a subscope, (), instead of -C/--directory
              echo -e "\t\t\t\t\tRestoring files to tarball"
              (cd $ens_dir && \
               tar --append \
                 --file $restore_tarball_path \
                 $filepaths_to_restore)

              # TODO: Delete files from the trash?
              # Maybe just leave them there? It's the trash bin after all....
            done
          done
        done
      done
    done
  done
done

echo -e "\nScript finished at `date`"
