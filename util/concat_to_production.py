# This script takes files from one archive, concatenate the files within a bin
# if necessary and moves the result to the production archive, creating a
# symlink for the BARRA2 suite to use.

## IMPORTS
from glob import glob
from os import makedirs, symlink, remove
from os.path import join, basename, normpath, exists
from shutil import move


## PARAMETERS
INPUT_DIR = "/g/data/hd50/barra2/data/obs/amsr2"
OUTPUT_DIR = "/g/data/hd50/barra2/data/obs/production"
OUTPUT_DIR = "/scratch/hd50/jt4085/production_test"

TYPE = "amsr"

OUTPUT_FILENAME = TYPE + "_{dt_str}.bufr"
TEMP_SUFFIX = ".temp"

SYMLINK_FILENAME = "AMSR2_1.bufr"


## SCRIPT
def main():

    years = glob(join(INPUT_DIR, "*"))
    years.sort()
    for y_dir in years:
        y = basename(y_dir)

        print("Year:", y)

        months = glob(join(y_dir, "*"))
        months.sort()
        for m_dir in months:
            m = basename(m_dir)

            print("\tMonth:", m)

            dts = glob(join(m_dir, "*"))
            dts.sort()
            for dt_dir in dts:
                dt = basename(dt_dir)

                print("\t\t", dt)

                # Find the corresponding output dir
                out_dir = join(OUTPUT_DIR, y, m, dt, "bufr", TYPE)

                # If the output directory doesn't exist create it.
                makedirs(out_dir, exist_ok=True)

                # Build the output file paths
                out_filename = OUTPUT_FILENAME.format(dt_str=dt)
                out_filepath = join(out_dir, out_filename)

                if exists(out_filepath):
                    print("\t\t\tOutput file already exists skipping")
                    continue

                temp_out_filepath = out_filepath + TEMP_SUFFIX

                # Concatenate the input files into a temp single file
                with open(temp_out_filepath, 'wb') as temp_out_file:
                    fs = glob(join(dt_dir, "*.bufr"))
                    fs.sort()
                    for f in fs:
                        f_name = basename(f)

                        with open(f, 'rb') as in_file:
                            temp_out_file.write(in_file.read())

                # Move the temp file to the output file
                move(temp_out_filepath, out_filepath)

                # Create a symlink to the outfile
                symlink_filepath = join(out_dir, SYMLINK_FILENAME)

                if exists(symlink_filepath):
                    # Delete the symlink if it already exists
                    remove(symlink_filepath)

                symlink(out_filepath, symlink_filepath)


if __name__ == "__main__":
    main()

