# This script takes files from one archive, concatenate the files within a bin
# if necessary and moves the result to the production archive, creating a
# symlink for the BARRA2 suite to use.

from datetime import datetime
## IMPORTS
from glob import glob
from os import makedirs, symlink, readlink, remove
from os.path import join, basename, exists, \
    getsize, islink, isabs, dirname
from shutil import move
from sys import argv

## PARAMETERS
# AMSR-2
if False:
    INPUT_DIR = "/g/data/hd50/barra2/data/obs/amsr2"
    TYPE = "amsr"
    OUTPUT_FILENAME = "AMSR2_1.bufr"

    SYMLINK_FILENAME = None
    CREATE_SYMLINK = False

    OLD_FILE_ARCHIVE_DIR = None

    START_DT = None
    END_DT = None

# JMA Winds
if True:
    INPUT_DIR = "/scratch/hd50/jt4085/jma_wind/bufr"
    TYPE = "satwind"
    OUTPUT_FILENAME = "JMAWINDS_1.bufr"

    CREATE_SYMLINK = False
    SYMLINK_FILENAME = "JMAWINDS_{index}.bufr"

    OLD_FILE_ARCHIVE_DIR = "/scratch/hd50/jt4085/jma_wind/old_production_bufrs"

    START_DT = datetime(year=int(argv[1]), month=1, day=1)
    END_DT = datetime(year=int(argv[1]), month=12, day=31, hour=23, minute=59)

# Sonde
if False:
    INPUT_DIR = "/scratch/hd50/jt4085/sonde/data-bufr-bins"
    TYPE = "sonde"
    OUTPUT_FILENAME = "TEMP_1.bufr"

    CREATE_SYMLINK = False
    SYMLINK_FILENAME = "TEMP_{index}.bufr"

    OLD_FILE_ARCHIVE_DIR = "/scratch/hd50/jt4085/sonde/old_production_bufrs"

    START_DT = datetime(year=int(argv[1]), month=1, day=1)
    END_DT = datetime(year=int(argv[1]), month=12, day=31, hour=23, minute=59)

# Output
OUTPUT_DIR = "/g/data/hd50/barra2/data/obs/production"
#OUTPUT_DIR = "/scratch/hd50/jt4085/production_test"

TEMP_SUFFIX = ".temp"


## FUNCTIONS
def archive_symlink_targets(symlink_path, archive_dir):
    print("\t\t\tArchiving old files...")

    # Find the symlinks that match symlink_path (can contain wildcards *)
    symlink_paths = glob(symlink_path)

    if len(symlink_paths) == 0:
        print("\t\t\t\tNo files found to archive.")

    for syml_path in symlink_paths:
        print("\t\t\t\tArchiving", basename(syml_path))
        if islink(syml_path):
            # Find the symlink's target
            target_path = readlink(syml_path)
            if not isabs(target_path):
                # Path is not absolute, join it to the sym link's dir
                target_path = join(dirname(syml_path), target_path)

            print("\t\t\t\t" + basename(syml_path),
                  "points to", basename(target_path))

            if exists(target_path):
                print("\t\t\t\tMoving", basename(target_path), "to archive.")
                # Move the target to the archive
                # Specify the full path to the dest so move will overwrite
                dest_path = join(archive_dir, basename(target_path))
                makedirs(archive_dir, exist_ok=True)
                move(target_path, dest_path)
            else:
                print("\t\t\t\tTarget doesn't exist:", target_path)

            # Symlink no longer points to anything so clean it up
            print("\t\t\t\tDeleting link", basename(syml_path))
            remove(syml_path)

            print()


## SCRIPT
def main():

    years = glob(join(INPUT_DIR, "*"))
    years.sort()
    for y_dir in years:
        y = basename(y_dir)

        print("Year:", y)

        if START_DT and int(y) < START_DT.year:
            print("\tBefore start year, skipping.")
            continue

        if END_DT and int(y) > END_DT.year:
            print("\tAfter end year, skipping.")
            continue

        months = glob(join(y_dir, "*"))
        months.sort()
        for m_dir in months:
            m = basename(m_dir)

            print("\tMonth:", m)

            if START_DT and \
                int(y) <= START_DT.year and int(m) < START_DT.month:
                print("\tBefore start month, skipping.")
                continue

            if END_DT and \
                int(y) >= END_DT.year and int(m) > END_DT.month:
                print("\tAfter end month, skipping.")
                continue

            dts = glob(join(m_dir, "*"))
            dts.sort()
            for dt_dir in dts:
                if ".broken" in dt_dir:
                    # Bad file I can't delete
                    continue

                dt = basename(dt_dir)

                # Add the missing 'Z' to the datetime if needed
                if dt[-1] != 'Z':
                    dt = dt + 'Z'

                print("\t\t", dt)

                datet = datetime(year=int(y), month=int(m),
                    day=int(dt[6:8]), hour=int(dt[9:11]))

                if START_DT and datet < START_DT:
                    print("\t\tBefore start day, skipping.")
                    continue

                if END_DT and datet > END_DT:
                    print("\t\tAfter end day, skipping.")
                    continue

                # Find the corresponding output dir
                out_dir = join(OUTPUT_DIR, y, m, dt, "bufr", TYPE)

                # If needed, archive files already in production
                # Archive files even if there aren't any new files.
                # TODO: This is not foolproof, if an entire bin is missing
                #  in the source dir then the corresponding dest will not be
                #  be archived.
                if OLD_FILE_ARCHIVE_DIR:
                    archive_dir = join(OLD_FILE_ARCHIVE_DIR,
                                       y, m, dt)

                    symlink_wildcard_path = join(out_dir,
                        SYMLINK_FILENAME.format(index="*"))

                    archive_symlink_targets(symlink_wildcard_path,
                                            archive_dir)

                fs = glob(join(dt_dir, "*.bufr"))
                fs.sort()
                if len(fs) > 0:
                    # If the output directory doesn't exist create it.
                    makedirs(out_dir, exist_ok=True)

                    # Build the output file paths
                    out_filename = OUTPUT_FILENAME.format(dt_str=dt)
                    out_filepath = join(out_dir, out_filename)

                    if exists(out_filepath):
                        print("\t\t\tOutput file already exists skipping")
#                        print("\t\t\tOutput file already exists overwriting")
                        continue

                    # Concatenate the input files into a temp single file
                    temp_out_filepath = out_filepath + TEMP_SUFFIX

                    with open(temp_out_filepath, 'wb') as temp_out_file:
                        for f in fs:
                            f_name = basename(f)

                            if getsize(f)>0:
                                print("\t\t\tCat-ing file:", f_name)
                                with open(f, 'rb') as in_file:
                                    temp_out_file.write(in_file.read())
                            else:
                                print("\t\t\tFile has a size of 0, skipping")

                    # Move the temp file to the output file
                    move(temp_out_filepath, out_filepath)


                    if CREATE_SYMLINK and SYMLINK_FILENAME:
                        # Replace the wildcard (if present) with the file index
                        symlink_file = SYMLINK_FILENAME.format(index=1)

                        # Create a symlink to the outfile
                        symlink_filepath = join(out_dir, symlink_file)

                        if exists(symlink_filepath):
                            # Delete the symlink if it already exists
                            remove(symlink_filepath)

                        symlink(out_filepath, symlink_filepath)
                else:
                    print("\t\t\tNo input files in bin.")



if __name__ == "__main__":
    main()

    print("Script finished.")

