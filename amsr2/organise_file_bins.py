#!/bin/python3

from os import getcwd, listdir, makedirs, rmdir
from os.path import isfile, isdir, join, basename
from re import search
from datetime import datetime, timedelta
from shutil import move
from argparse import ArgumentParser

# Datetime format in JAXA filenames (201801010007 -> yyyymmddhhss
JAXA_DT_FORMAT = "%Y%m%d%H%M"

# Datetime format to use for the bin directories
BIN_DIR_FORMAT = "%Y%m%dT%H%MZ"
# Currently, following the cylc datetime format


def get_datetime_from_filename(filename, dt_format=JAXA_DT_FORMAT):
    # Just look for a 12-digit number in the filename (YYYYMMDDHHmm)
    x = search("\\d{12}", filename)

    if x:
        return datetime.strptime(x.group(), dt_format)


def flatten_dir(directory_to_flatten, dry_run=True):
    # Get a list of directories
    dir_list = [f for f in listdir(directory_to_flatten) if isdir(join(directory_to_flatten, f))]

    if len(dir_list) == 0:
        print("No directories found in", directory_to_flatten)
        print("Exiting.")
        return

    for i, d in enumerate(dir_list):
        print("{} of {} dirs: {}".format(i + 1, len(dir_list), d))

        subdir = join(directory_to_flatten, d)

        # Get a list of files within each directory.
        file_list = [f for f in listdir(subdir) if isfile(join(subdir, f))]

        for j, f in enumerate(file_list):
            print("\t{} of {} files in {}: {}".format(j + 1, len(file_list), d, f))

            # Move each file up to the parent directory.
            source = join(subdir, f)
            dest = join(directory_to_flatten, f)

            print("\tSource:", source)
            print("\tDest:", dest)

            if dry_run:
                print("\tDry run, skipping file/dir alterations.")
            else:
                move(source, dest)

        # Delete the now empty directory.
        print("Deleting directory: ", d)

        if dry_run:
            print("\tDry run, skipping file/dir alterations.")
        else:
            rmdir(subdir)


def bin_dir(dir_to_bin, dry_run):
    # Get a list of files in the current working directory.
    file_list = [f for f in listdir(dir_to_bin) if isfile(join(dir_to_bin, f))]

    if len(file_list) == 0:
        print("No files found in", dir_to_bin)
        print("Exiting.")
        return

    # Assume that sorting puts the files in chronological order.
    file_list.sort()

    # If the datetime for the first bin is not given use the datetime in the first file.
    starting_bin_dt = None
    if not starting_bin_dt:
        starting_bin_dt = get_datetime_from_filename(file_list[0])

        # Align the starting bin to the hour
        starting_bin_dt = starting_bin_dt.replace(minute=0, second=0, microsecond=0)

    current_bin_dt = starting_bin_dt
    bin_interval_td = timedelta(hours=6)
    for i, f in enumerate(file_list):
        print("{} of {}: {}".format(i + 1, len(file_list), f))

        f_dt = get_datetime_from_filename(f)
        if current_bin_dt + bin_interval_td < f_dt:
            # We've left the bin

            # Allow for multiple bin increments in case there's a gap in files
            while current_bin_dt + bin_interval_td <= f_dt:
                current_bin_dt += bin_interval_td

        # Move the file into the bin.
        filename = basename(f)
        current_bin_dir = join(dir_to_bin, current_bin_dt.strftime(BIN_DIR_FORMAT))

        source = join(dir_to_bin, f)
        dest = join(current_bin_dir, filename)

        print("\tSource:", source)
        print("\tDest:", dest)

        if dry_run:
            print("\tDry run, skipping file/dir alterations.")
        else:
            makedirs(current_bin_dir, exist_ok=True)
            move(source, dest)


def prompt_to_continue():
    print("This script moves files, are you sure you want to proceed?")
    print("A dry-run/practise-run of the script can be performed without "
          "altering any files by using the command line option --dry-run")
    print("This prompt can be suppressed with the --force command line option.")
    print()
    print("Press Y or y to proceed or anything else to abort.")

    inp = input().lower()

    if inp != "y":
        print("Aborting.")
        exit()


def parse_args():
    parser = ArgumentParser(prog="organise_file_bins.py",
                            description="This utility bins the supplied files "
                                        "into directories or flattens binned "
                                        "data out of their directories."
                                        "Initial intended for use at the BoM with data files from JAXA satellites."
                                        "Author: Joshua Torrance")

    parser.add_argument("-d", "--dir", nargs="?", default=getcwd(),
                        help="The path to the directory to work from. "
                             "Defaults to the current working directory.")
    parser.add_argument("-m", "--mode", nargs="?", default="flatten", choices=["flatten", "bin"],
                        help="The mode of the script, either bin files "
                             "into directories or flatten files out of directories.")
    parser.add_argument("--dry-run",
                        help="Perform a dry run of the file organisation. "
                             "I.e. print actions to be taken with moving any files.",
                        action="store_true")
    parser.add_argument("-f", "--force",
                        help="Run this script without prompting for confirmation.",
                        action="store_true")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    mode = args.mode
    directory = args.dir
    do_dry_run = args.dry_run

    if not (args.force or args.dry_run):
        prompt_to_continue()

    if mode == "flatten":
        flatten_dir(directory, do_dry_run)
    elif mode == "bin":
        bin_dir(directory, do_dry_run)
    else:
        print("Unknown mode. Exiting.")
