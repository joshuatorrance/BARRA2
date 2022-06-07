# This script is intended to be used to check the "vital statistics" of a
# sonde bufr file to allow comparison between supposedly identical files.
#
# Joshua Torrance

from collections.abc import Iterable
from datetime import datetime
# IMPORTS
from glob import glob
from os import remove
from os.path import exists, basename
from shutil import copyfile
from subprocess import run
from sys import path

from matplotlib import pyplot as plt
from numpy import array, concatenate

# Import custom modules
path.insert(1, "/g/data/hd50/jt4085/BARRA2/util/bufr")
from eccodes_wrapper import BufrFile

# PARAMETERS
# Barra region
BARRA_LEFT = 90.00
BARRA_RIGHT = 210.0 - 360
BARRA_BOTTOM = -60.00
BARRA_TOP = 15.00
barra_filter = (BARRA_LEFT, BARRA_RIGHT, BARRA_BOTTOM, BARRA_TOP)


YEAR = 2007
MONTH = 1
DAY = 21
HOUR = 18
INPUT_DIR1 = "/scratch/hd50/jt4085/sonde/data-bufr-bins"
INPUT_FILE_PATH1 = "{input_dir}/{year}/{month:02}/{year}{month:02}{day:02}T{hour:02}00Z/*.bufr"
#INPUT_FILE_PATH = "/scratch/hd50/jt4085/sonde/data-bufr/ZZXUAICE019-data.bufr"

INPUT_DIR2 = "/g/data/hd50/barra2/data/obs/production"
INPUT_FILE_PATH2 = "{input_dir}/{year}/{month:02}/{year}{month:02}{day:02}T{hour:02}00Z/bufr/sonde/TEMP_*.bufr"

STATION_LIST_PATH = "/g/data/hd50/barra2/data/obs/igra/doc/igra2-station-list.txt"

TEMP_FILE_DIR = "/scratch/hd50/jt4085/tmp"
TEMP_FILE_PATH = TEMP_FILE_DIR + "/temporary.file"


# METHODS
def get_obs_count(filepath, geo_filter=None):
    # geo_filter should be (left, right, bottom, top)
    obs_count = 0
    with BufrFile(filepath) as bufr:
        for msg in bufr.get_messages():
            if geo_filter:
                lat, lon = msg.get_locations()
                if lat=="MISSING" or lon=="MISSING":
                    # Something has gone awry
                    print("Missing lat or lon for:", filepath)
                    continue

                left, right, bottom, top = geo_filter

                if not (
                    bottom < lat < top and \
                    (left < right and (left < lon < right) or \
                     left > right and (left < lon or lon < right))):
                    # ^ Watch out for left/right looping
                    # Outside filter, skip
                    continue

            obs_count += msg.get_obs_count()

    return obs_count


def get_locations(filepath):
    latitude = []
    longitude = []
    with BufrFile(filepath) as bufr:
        for msg in bufr.get_messages():
            lat, lon = msg.get_locations()

            latitude.append(lat)
            longitude.append(lon)

    latitude = [lat if lat != "MISSING" else float('NaN')
                for lat in latitude]
    longitude = [lon if lon != "MISSING" else float('NaN')
                 for lon in longitude]

    return array(latitude), array(longitude)


def grep(file_path, regex):
    # Just use grep itself.
    ret = run(["grep", regex, file_path], capture_output=True)

    # Decode from bytes string and return
    return ret.stdout.decode("utf-8")


def get_station_location(station_name):
    station_line = grep(STATION_LIST_PATH, station_name)    

    lat = float(station_line[12:20])
    lon = float(station_line[21:30])

    # Check if lat/lon are mobile stations
    if lat == -98.8888:
        lat = None
    if lon == -998.8888:
        lon = None

    return lat, lon


def get_attribute_number_array(file_path, key):
    arr = None
    with BufrFile(file_path) as bufr:
        for msg in bufr.get_messages():
            values = msg.get_value(key)

            if not isinstance(values, Iterable):
                values = [values]
            elif values == "MISSING":
                values = [float("NaN")]

            if arr is not None:
                arr = concatenate((arr, values))
            else:
                arr = values

    return arr


# SCRIPT
def main():
    dts = []
    prod_obs_count = []
    converted_obs_count = []
#    for y in range(1978, 1984):
    for y in [1978]:
        for m in range(1, 2):
            for d in range(1, 31):
                for h in [0, 6, 12, 18]:
                    dts.append(datetime(year=y, month=m, day=d, hour=h))

                    input_1 = INPUT_FILE_PATH1.format(
                        input_dir=INPUT_DIR1, year=y, month=m,
                        day=d, hour=h)

                    input_2 = INPUT_FILE_PATH2.format(
                        input_dir=INPUT_DIR2, year=y, month=m,
                        day=d, hour=h)

                    print(input_1)
                    print(input_2)

                    print()
                    obs_count = 0
                    for f in glob(input_2):
                        print(f)
                        print(basename(f))

                        # Make a copy of the file to interrogate
                        copyfile(f, TEMP_FILE_PATH)
                        f = TEMP_FILE_PATH

                        print("\tGetting lat and lon...", end="")
                        lat, lon = get_locations(f)
                        lon = [l + 360 if l<0 else l for l in lon]
                        plt.scatter(lon, lat, color="C0", marker="x",
                                    label="Production")
                        print("done.")

                        obs_c = get_obs_count(f, geo_filter=(barra_filter))
                        obs_count += obs_c

                        print("\tObs Count:", obs_c)


#                        try:
#                            air_temp = get_attribute_number_array(f, "airTemperature")
#                        except ValueError as e:
                            # This is failing sometimes.
                            # TODO: Figure out why I can't access attributes
                            #  sometimes.
                            # For now fill the array with NaN
#                            print("\tFailed to get air_temp:", e)
#                            air_temp = full(lat.shape, float("nan"))

#                        print("air_temp")
#                        print(air_temp)
#                        print("\tMean:", nanmean(air_temp))
#                        print("\tStd:", nanstd(air_temp))
#                        print("\tMin:", nanmin(air_temp))
#                        print("\tMax:", nanmax(air_temp))

                    prod_obs_count.append(obs_count)

                    print()
                    obs_count = 0
                    for f in glob(input_1):
                        print(basename(f))

                        print("\tGetting lat and lon...", end="")
                        lat, lon = get_locations(f)

                        lon = [l + 360 if l<0 else l for l in lon]

                        if lon is not None and lat is not None:
                            plt.scatter(lon, lat, marker='+', color="C1",
                                        label="Converted")

                        print("done.")

                        obs_c = get_obs_count(f, geo_filter=barra_filter)
                        obs_count += obs_c

                        print("\tObs Count:", obs_c)

                    converted_obs_count.append(obs_count)

        if exists(TEMP_FILE_PATH):
            remove(TEMP_FILE_PATH)

    #    plt.xlim((-180, 180))
    #    plt.xlim((0, 360))
    #    plt.ylim((-90, 90))
        plt.xlim((BARRA_LEFT-5, BARRA_RIGHT+360+5))
        plt.ylim((BARRA_BOTTOM-5, BARRA_TOP+5))

        plt.axhline(BARRA_BOTTOM, linestyle='-', color='r')
        plt.axhline(BARRA_TOP, linestyle='-', color='r')
        plt.axvline(BARRA_LEFT, linestyle='-', color='r')
        plt.axvline(BARRA_RIGHT, linestyle='-', color='r')
        plt.axvline(BARRA_RIGHT+360, linestyle='-', color='r')

        plt.title(str(y))
        plt.xlabel("Longitude (degrees)")
        plt.ylabel("Latitude (degrees)")

        # Dedup labels
        handles, labels = plt.gca().get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        plt.legend(by_label.values(), by_label.keys(), loc='lower right')

        plt.figure()
        plt.plot(dts, converted_obs_count, color="C1", label="Converted")
        plt.plot(dts, prod_obs_count, color="C0", label="Production")

        print("Datetime, Converted, Production")
        for dt, con, pro in zip(dts, converted_obs_count, prod_obs_count):
            print(", ".join([dt.strftime("%Y%m%dT%H%M"), str(con), str(pro)]))

        plt.xlabel("Datetime")
        plt.ylabel("Observation Count")

        plt.legend()

    plt.show()


if __name__ == "__main__":
    main()
