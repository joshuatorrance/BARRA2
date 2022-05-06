# Script to orchestrate sonde bufr conversion.
#
# This script matches raw sonde data to bias correction data
# and converts the result to a .bufr files.
#
# This script is intended for use with the IGRA raw data, the
# raobcore/ERA5 bias data for the BARRA2 project.
#
# Author: Joshua Torrance (joshua.torrance@bom.gov.au)

# IMPORTS
from sys import argv
from glob import glob
from os import remove as delete_file, rmdir
from os.path import join, basename, exists, splitext
from shutil import move
from zipfile import ZipFile
from subprocess import run
from netCDF4 import Dataset
from datetime import datetime
from multiprocessing.pool import Pool
from functools import partial
from re import search

from sonde_bufr_converter import do_conversion
from igra2_sonde_type import meta_sonde_type_dict_list


# PARAMETERS
# Multiprocessing
N_CPU = 1

# IGRA Data Details
IGRA_FILE_DIR = "/g/data/hd50/barra2/data/obs/igra"

IGRA_STATION_LIST_PATH = join(IGRA_FILE_DIR, "doc/igra2-station-list.txt")
IGRA_METADATA_PATH = join(IGRA_FILE_DIR, "doc/igra2-metadata.txt")

# Sonde txts are IGRA, and not bias corrected, they also start as ZIPs
SONDE_TXT_INPUT_DIR = join(IGRA_FILE_DIR, "data-por")
SONDE_TXT_ZIP_EXTENSION = ".zip"
SONDE_TXT_EXTENSION = ".txt"

# A temporary directory to unpack the zips into
# Temp file can be on scratch
TEMP_DIR = "/scratch/hd50/jt4085/sonde/temp"

# raobcore/ERA5 Details
ERA5_FILE_DIR = "/g/data/hd50/barra2/data/obs/raobcore"

# ERA5 netCDFs are the bias corrected data.
SONDE_NC_INPUT_DIR = join(ERA5_FILE_DIR, "ERA5_v7")
SONDE_NC_EXTENSION = ".nc"

# Template .bufr file, used to create new .bufrs
TEMPLATE_BUFR = "/g/data/hd50/jt4085/BARRA2/sonde/data/temp.bufr"

# Output directory
BUFR_EXTENSION = ".bufr"
OUTPUT_DIR = "/scratch/hd50/jt4085/sonde/data-bufr"

# Barra region
BARRA_LEFT = 90.00
BARRA_RIGHT = 210.0 - 360
BARRA_BOTTOM = -60.00
BARRA_TOP = 15.00


# FUNCTIONS
def grep(file_path, regex):
    # Just use grep itself.
    ret = run(["grep", regex, file_path], capture_output=True)

    # Decode from bytes string and return
    return ret.stdout.decode("utf-8")


def get_bias_correction_stations(bias_directory,
                                 bias_extension=SONDE_NC_EXTENSION):
    # Get a list of bias files
    bias_files = glob(join(bias_directory,
                           "*" + bias_extension))

    # Look through each files and determine the station name
    bias_dicts = []
    for f_bias in bias_files:
        with Dataset(f_bias, "r", format="NETCDF4") as nc:
            station_name = nc.__dict__['Stationnname'].strip()

        bias_dicts.append({"path": f_bias,
                           "station name": station_name})

    return bias_dicts


def get_station_name_from_code(station_code,
                               station_list_file=IGRA_STATION_LIST_PATH):
    # Get the name of the station matching the station code
    result = grep(station_list_file, station_code)
    if result == '':
        raise ValueError("Station code ({}) not found.".format(station_code))
    else:
        return str(result[41:71].strip())


def get_station_location(station_code,
                         station_list_file=IGRA_STATION_LIST_PATH):
    station_line = grep(station_list_file, station_code)

    lat = float(station_line[12:20])
    lon = float(station_line[21:30])

    # Check if lat/lon are mobile stations
    if lat == -98.8888:
        lat = None
    if lon == -998.8888:
        lon = None

    return lat, lon


def is_station_in_barra2_region(station_code):
    lat, lon = get_station_location(station_code)

    if lat is None or lon is None:
        # Mobile station, let's say it is in the region.
        return True
    elif BARRA_BOTTOM < lat < BARRA_TOP and \
        ( (BARRA_LEFT < BARRA_RIGHT) and \
          (BARRA_LEFT < lon < BARRA_RIGHT) ) or \
        ( (BARRA_LEFT > BARRA_RIGHT) and \
          (BARRA_LEFT < lon or lon < BARRA_RIGHT) ):
        return True
    else:
        return False


def get_station_metadata(station_code):
    meta_data = grep(IGRA_METADATA_PATH, station_code + ".*SONDE MODEL")

    sonde_types = []
    for l in meta_data.split('\n'):
        if len(l) > 0:
            year = int(l[84:88])
            month = int(l[89:91])
            month = month if month!=99 else 1
            day = int(l[92:94])
            day = day if day!=99 else 1
            hour = int(l[95:97])
            hour = hour if hour!=99 else 1

            dt = datetime(year=year,
                          month=month,
                          day=day,
                          hour=hour)

            event = l[100:119]

            sonde_model_before = l[123:163].strip()
            sonde_model_after = l[168:208].strip()

            if sonde_model_after and \
                sonde_model_after!="" and sonde_model_after!="NONE":
                sonde_model = sonde_model_after
            else:
                sonde_model = sonde_model_before

            print(l)
            print("\tDatetime:", dt)
            print("\tEvent:", event)
            print("\tBefore:", sonde_model_before)
            print("\tAfter:", sonde_model_after)
            print("\tModel:", sonde_model)

            for d in meta_sonde_type_dict_list:
                reg = d["regex"]
                if reg and search(d["regex"], sonde_model):
                    sonde_name = d["name"]
                    sonde_id = d["id"]
                    print("\tSonde name:", sonde_name)
                    print("\tSonde ID:", sonde_id)

                    break
            else:
                print("\tNo match found!")
                exit()

            sonde_types.append((dt, sonde_id))

            print()

    return sonde_types

def _process_zip(f_zip, biases):
    print(basename(f_zip))

    # Unpack the zip
    zip_dir = join(TEMP_DIR, basename(f_zip))
    with ZipFile(f_zip, 'r') as z:
        z.extractall(zip_dir)

    # There can be multiple files in the zip.
    txt_files = glob(join(zip_dir, "*" + SONDE_TXT_EXTENSION))
    for txt_file in txt_files:
        print("Processing", basename(txt_file))

        # Check if the output file already exists.
        file_name_sans_extension, _ = splitext(basename(f_zip))
        output_file_name = file_name_sans_extension + BUFR_EXTENSION
        output_file = join(OUTPUT_DIR, output_file_name)

        if exists(output_file):
            print("Output file ({}) already exists, skipping..."
                  .format(basename(output_file)))
        else:
            # Get the station code from the filename
            station_code = basename(str(txt_file))[:11]

            # Is the station ion the BARRA2 region?
            if not is_station_in_barra2_region(station_code):
                print("\tStation not in BARRA2 region, skipping.")
                continue
            else:
                print("\tStation in BARRA2 region, processing...")

            # Determine the matching station name
            station_name = get_station_name_from_code(station_code)

            # Is there a bias for this station?
            bias_path = None
            for b in biases:
                if b['station name'] == station_name:
                    bias_path = b['path']
                    break

            # Get the metadata for that station so we know the radiosonde type
            # This has proved not feasible. Metadata is too messy and
            #  doesn't line up well with radiosondeTypes.
            # Left for no for posterity. TODO: Delete?
            #something = get_station_metadata(station_code)

            # We now know the filename for the raw sonde data and for
            #   the bias correction (if it exists)
            # Output to the temp directory
            temp_output_file = join(zip_dir, output_file_name)
            do_conversion(txt_file, bias_path, temp_output_file, TEMPLATE_BUFR)

            # With conversion complete move to the output path
            move(temp_output_file, output_file)

    # Delete everything in the temp directory
    for f in glob(join(zip_dir, '*')):
        delete_file(f)

    # Delete the temp directory
    if exists(zip_dir):
        rmdir(zip_dir)


# SCRIPT
def main():
    # Multiprocessing hax
    # Take command line arg to indicate which process this is
    num_processes = 100
    process_n = int(argv[1])

    print("Running python script, {} of {}".format(process_n, num_processes))

    # Build a dictionary of bias correction files with their station names.
    biases = get_bias_correction_stations(SONDE_NC_INPUT_DIR)

    # Raw data/txt files are compressed into single-file zips
    sonde_txt_zip_files = glob(join(SONDE_TXT_INPUT_DIR,
                                    "*" + SONDE_TXT_ZIP_EXTENSION))

    # Input dir has some other files too.
    files_to_ignore = glob(join(SONDE_TXT_INPUT_DIR, "igra2-*.txt.zip"))
    for f in files_to_ignore:
        sonde_txt_zip_files.remove(f)

    # Separate the list into chunks based on command line arg
    list_len = len(sonde_txt_zip_files)
    from math import ceil
    chunk_size = ceil(list_len / num_processes)
    start = process_n * chunk_size
    end = start + chunk_size
    sonde_txt_zip_files = sonde_txt_zip_files[start:end]

    with Pool(N_CPU, maxtasksperchild=1) as pool:
        f = partial(_process_zip, biases=biases)
        pool.map(f, sonde_txt_zip_files)

    # Delete the temp directory
    if exists(TEMP_DIR):
        rmdir(TEMP_DIR)

    print("Script finished at", datetime.now())


if __name__ == "__main__":
    main()

