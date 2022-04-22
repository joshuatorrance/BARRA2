# This script is intended to be used to check the "vital statistics" of a
# sonde bufr file to allow comparison between supposedly identical files.
#
# Joshua Torrance

# IMPORTS
from glob import glob
from numpy import array, concatenate, nanmean, nanstd, nanmax, nanmin
from os.path import exists, basename
from subprocess import run
from datetime import datetime
from collections.abc import Iterable
import eccodes as ecc
from sys import path
from matplotlib import pyplot as plt

# Import custom modules
path.insert(1, "/g/data/hd50/jt4085/BARRA2/util/bufr")
from eccodes_wrapper import BufrFile

# PARAMETERS
YEAR = 2007
MONTH = 1
DAY = 21
HOUR = 18
INPUT_DIR1 = "/scratch/hd50/jt4085/sonde/data-bufr-bins"
INPUT_FILE_PATH1 = "{input_dir}/{year}/{month:02}/*-{year}{month:02}{day:02}{hour:02}00.bufr"
#INPUT_FILE_PATH = "/scratch/hd50/jt4085/sonde/data-bufr/ZZXUAICE019-data.bufr"

INPUT_DIR2 = "/g/data/hd50/barra2/data/obs/production"
INPUT_FILE_PATH2 = "{input_dir}/{year}/{month:02}/{year}{month:02}{day}T{hour:02}00Z/bufr/sonde/TEMP_*.bufr"

STATION_LIST_PATH = "/g/data/hd50/barra2/data/obs/igra/doc/igra2-station-list.txt"

# METHODS
def get_obs_count(filepath):
    with BufrFile(filepath) as bufr:
        obs_count = bufr.get_obs_count()

    return obs_count

def get_locations(filepath):
    latitude = []
    longitude = []
    with BufrFile(filepath) as bufr:
        for msg in bufr.get_messages():
            lat, lon = msg.get_locations()

            latitude.append(lat)
            longitude.append(lon)

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
            elif values=="MISSING":
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
    for y in [2007]:
        for m in range(1, 2):
            for d in range(15, 16):
                for h in [0, 6, 12, 18]:
                    dts.append(datetime(year=y, month=m, day=d, hour=h))

                    input_1 = INPUT_FILE_PATH1.format(
                        input_dir=INPUT_DIR1, year=y, month=m,
                        day=d, hour=h)

                    input_2 = INPUT_FILE_PATH2.format(
                        input_dir=INPUT_DIR2, year=y, month=m,
                        day=d, hour=h)

                    print()
                    obs_count = 0
                    for f in glob(input_2):
                        print(f)
                        print(basename(f))

                        print("\tGetting lat and lon...", end="")
                        lat, lon = get_locations(f)
                        plt.scatter(lon, lat, color="C0")
                        print("done.")

                        obs_c = get_obs_count(f)
                        obs_count += obs_c

                        print("\tObs Count:", obs_c)

                        airTemp = get_attribute_number_array(f, "airTemperature")
                        print("airTemp")
                        print(airTemp)
                        print("\tMean:", nanmean(airTemp))
                        print("\tStd:", nanstd(airTemp))
                        print("\tMin:", nanmin(airTemp))
                        print("\tMax:", nanmax(airTemp))

                    prod_obs_count.append(obs_count)

                    print()
                    obs_count = 0
                    for f in glob(input_1):
                        print(basename(f))

                        station_name =  basename(f)[:11]
                        station_lat, station_lon = \
                            get_station_location(station_name)

                        print("\tGetting lat and lon...", end="")
                        lat, lon = get_locations(f)
                        plt.scatter(lon, lat, marker='+', color="C1")
                        plt.scatter(station_lon, station_lat,
                                    marker='x', color="C2")
                        print("done.")

                        obs_c = get_obs_count(f)
                        obs_count += obs_c

                        print("\tObs Count:", obs_c)

                    converted_obs_count.append(obs_count)


    plt.xlim((-180, 180))
    plt.ylim((-90, 90))

    plt.axhline(-60, linestyle='-', color='r')
    plt.axhline(15, linestyle='-', color='r')
    plt.axvline(90, linestyle='-', color='r')
    plt.axvline(210-360, linestyle='-', color='r')

    plt.xlabel("Longitude (degrees)")
    plt.ylabel("Latitude (degrees)")

    plt.figure()
    plt.plot(dts, converted_obs_count, label="Converted")
    plt.plot(dts, prod_obs_count, label="Production")

    plt.xlabel("Datetime")
    plt.ylabel("Observation Count")

    plt.legend()

    plt.show()


if __name__ == "__main__":
    main()

