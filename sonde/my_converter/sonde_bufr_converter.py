#!/bin/python2

# MODULES
# module load python2
# module load eccodes
# module load pythonlib/netCDF4

# This is a Python 3 rewrite of Tan's Sonde/BUFR converter by JT.
#
# Python 3 installation of eccodes/grib-api seems broken.
# Changing back to Py2 with as little changes as I can.


# IMPORTS
from argparse import ArgumentParser
from logging import basicConfig as loggingConfig, info
from sonde import SondeTXT, SondeNC, SondeBUFR


# METHODS
def parse_args():
    parser = ArgumentParser(prog="sonde_bufr_converter.py",
                            description="This script converts sonde data "
                                        ".BUFR format.\n"
                                        "Author: Joshua Torrance")

    parser.add_argument("-i", "--input",
                        required=True,
                        nargs=2,
                        help="File paths for the two input files, the first "
                             "a .txt and the second a .nc.")

    parser.add_argument("-t", "--template",
                        required=True,
                        help="A BUFR template file to use to initialise the output.")

    parser.add_argument("-o", "--output",
                        required=True,
                        help="File path to the desired output file.")

    parser.add_argument("-v", "--verbose",
                        required=False,
                        action="store_true",
                        help="Enable verbose logging.")

    return parser.parse_args()


# MAIN
def main():
    args = parse_args()

    if args.verbose:
        loggingConfig(level="DEBUG")

    path_input_txt = args.input[0]
    path_input_nc = args.input[1]
    path_template_bufr = args.template
    path_output_bufr = args.output

    info("Input files: {}, {}".format(path_input_txt, path_input_nc))
    info("BUFR Template file: {}".format(path_template_bufr))
    info("Output file: {}".format(path_output_bufr))

    # Load the data in the .nc file
    sonde_nc = SondeNC()
    sonde_nc.read(path_input_nc)

    sonde_txt = SondeTXT()
    with open(path_input_txt, 'r') as file_txt:
        with open(path_output_bufr, 'wb') as file_bufr:
            # TODO: This "write bufr file" loop should be abstracted into
            #   a SondeBUFR method
            not_finished = sonde_txt.read(file_txt)
            while not_finished:
                sonde_bufr = SondeBUFR(path_template_bufr)

                sonde_bufr.write_temp(file_bufr, sonde_txt, sonde_nc)

                not_finished = sonde_txt.read(file_txt)

                sonde_bufr.close()


if __name__ == "__main__":
    main()


