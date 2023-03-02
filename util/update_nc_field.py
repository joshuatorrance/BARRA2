# This script is intended to simple update fields in a netCDF file and
# save the file.
#
# Use with something like find, e.g.
# find /path/to/data -type f -name *.nc \
#    -exec python3 /path/to/script/update_nc_field.py \
#        -d -f "field1" "field2" -v "value1" "value2" -i {} \;
#
# Remove -d (dry-run) to actually change data.
#
# Tested with:
# module load conda/analysis3-22.04

## IMPORTS
from os.path import basename
from argparse import ArgumentParser
from netCDF4 import Dataset


## PARAMETERS

## MAIN
def parse_args():
    parser = ArgumentParser(prog="update_nc_field.py",
                            description="This updates a field in a given netCDF file."
                                        "\n\n"
                                        "Author: Joshua Torrance")

    parser.add_argument("-i", "--input-files", nargs="+", required=True,
                        help="File/s to update")
    parser.add_argument("-f", "--fields", nargs="+", required=True,
                        help="Field/s to update")
    parser.add_argument("-v", "--values", nargs="+", required=True,
                        help="Value/s to update the field/s with (same order)")
    parser.add_argument("-d", "--dry-run", required=False, action="store_true",
                        help="Dry run of script, print values instead of "
                             "changing them.")

    return parser.parse_args()


def main():
    args = parse_args()

    input_files = args.input_files
    field_names = args.fields
    field_values = args.values
    is_dry_run = args.dry_run

    assert len(field_names) == len(field_values), \
        "Number of field names and values needs to be the same."

    # Get the length of the longest field name for pretty printing
    longest_name_len = max([len(s) for s in field_names])

    # We don't need to open the file with edit for a dry run.
    read_mode = "r" if is_dry_run else "r+"

    for f in input_files:
        print(basename(f))
        with Dataset(f, read_mode) as ds:
            for f, v in zip(field_names, field_values):
                if is_dry_run:
                    # Pad the name string to align values.
                    print(f.ljust(longest_name_len), ":", getattr(ds, f))
                else:
                    setattr(ds, f, v)
        print()


if __name__ == "__main__":
    main()

