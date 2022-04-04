#!/bin/python2

# MODULES
# module load python2
# module load eccodes
# module load pythonlib/netCDF4

# This is a Python 3 rewrite of Tan's Sonde/BUFR converter by JT.
#
# Python 3 installation of eccodes/grib-api seems broken.
# Changing back to Py2 with as little changes as I can.
#
# The following loads python3 and eccodes (something wrong with
# the default py3 module).
# pythonlib/netcCDF4 doesn't work with py3 though. :(
# module load python3/3.8.5
# module load eccodes3
#


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
                        nargs="?",
                        help="File path for the input file, a .txt containing "
                             "the raw sonde data from IGRA.")

    parser.add_argument("-b", "--bias",
                        required=False,
                        nargs="?",
                        help="File path to the optional netCDF file (.nc) that "
                             "contains the bias correction.")

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

    path_input_txt = args.input
    path_input_nc = args.bias
    path_template_bufr = args.template
    path_output_bufr = args.output

    info("Input file: {}".format(path_input_txt))
    info("Bias file: {}".format(path_input_nc))
    info("BUFR Template file: {}".format(path_template_bufr))
    info("Output file: {}".format(path_output_bufr))

    do_conversion(path_input_nc, path_input_txt, path_output_bufr, path_template_bufr)


def do_conversion(path_input_txt, path_input_nc, path_output_bufr, path_template_bufr):
    """
    :param path_input_txt: The input .txt file for the raw sonde data.
    :param path_input_nc: The optional input .nc file for the bias correction.
    :param path_output_bufr: The output bufr file
    :param path_template_bufr: The template bufr file.
    :return:
    """
    if path_input_nc:
        # Load the data in the .nc file
        sonde_nc = SondeNC()
        sonde_nc.read(path_input_nc)
    else:
        sonde_nc = None
    sonde_txt = SondeTXT()
    with open(path_input_txt, 'r') as file_txt:
        sonde_txt.read(file_txt)
    with open(path_output_bufr, 'wb') as file_bufr:
        for obs in sonde_txt.observations:
            sonde_bufr = SondeBUFR(path_template_bufr, obs.n_levels)

            sonde_bufr.write_bufr_message(file_bufr, obs, sonde_nc)

            sonde_bufr.close()


if __name__ == "__main__":
    main()
