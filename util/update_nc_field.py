# This script is intended to simple update a field in a netCDF file and
# save the file.
#
# Tested with:
# module load conda/analysis3-22.04

## IMPORTS
from argparse import ArgumentParser
from netCDF4 import Dataset


## PARAMETERS

## MAIN
def parse_args():
    parser = ArgumentParser(prog="update_nc_field.py",
                            description="This updates a field in a given netCDF file."
                                        "\n\n"
                                        "Author: Joshua Torrance")

    parser.add_argument("-i", "--input-file", nargs="?", required=True,
                        help="File to update")
    parser.add_argument("-f", "--field", nargs="?", required=True,
                        help="Field to update")
    parser.add_argument("-v", "--value", nargs="?", required=True,
                        help="Value to update the field to")

    return parser.parse_args()


def main():
    args = parse_args()

    input_file = args.input_file
    field_name = args.field
    field_value = args.value

    with Dataset(input_file, 'r+') as ds:
        rcm_model = getattr(ds, "rcm_model")
        if rcm_model == "BARRA-RE2" or rcm_model == "BARRA-R2":
            setattr(ds, field_name, field_value)


if __name__ == "__main__":
    main()
