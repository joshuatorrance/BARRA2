# This script is intended to be used to check the "vital statistics" of a
# sonde bufr file to allow comparison between supposedly identical files.
#
# Joshua Torrance

# IMPORTS
from glob import glob
from numpy import array, concatenate, nanmean, nanstd, nanmax, nanmin, full
from os import remove
from os.path import exists, basename
from shutil import copyfile
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

TEMP_FILE_DIR = "/scratch/hd50/jt4085/tmp"
TEMP_FILE_PATH = TEMP_FILE_DIR + "/temporary.file"


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
    for y in [2019]:
#    for y in [2007, 2009, 2010, 2011, 2012, 2013, \
#              2016, 2017, 2018, 2019, 2021, 2022]:
        for m in range(1, 2):
            for d in range(1, 32):
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

                        # Make a copy of the file to interrogate
                        copyfile(f, TEMP_FILE_PATH)
                        f = TEMP_FILE_PATH

                        print("\tGetting lat and lon...", end="")
                        lat, lon = get_locations(f)
                        lon = [l + 360 if l<0 else l for l in lon]
                        plt.scatter(lon, lat, color="C0", marker="x",
                                    label="Production")
                        print("done.")

                        obs_c = get_obs_count(f)
                        obs_count += obs_c

                        print("\tObs Count:", obs_c)


                        try:
                            air_temp = get_attribute_number_array(f, "airTemperature")
                        except ValueError as e:
                            # This is failing sometimes.
                            # TODO: Figure out why I can't access attributes
                            #  sometimes.
                            # For now fill the array with NaN
                            print("\tFailed to get air_temp:", e)
                            air_temp = full(lat.shape, float("nan"))

                        print("air_temp")
                        print(air_temp)
                        print("\tMean:", nanmean(air_temp))
                        print("\tStd:", nanstd(air_temp))
                        print("\tMin:", nanmin(air_temp))
                        print("\tMax:", nanmax(air_temp))

                    prod_obs_count.append(obs_count)

                    print()
                    obs_count = 0
                    for f in glob(input_1):
                        print(basename(f))

                        station_name = basename(f)[:11]
                        station_lat, station_lon = \
                            get_station_location(station_name)

                        print("\tGetting lat and lon...", end="")
                        lat, lon = get_locations(f)

                        lon = [l + 360 if l<0 else l for l in lon]

                        if lon is not None and lat is not None:
                            plt.scatter(lon, lat, marker='+', color="C1",
                                        label="Converted")

                        if station_lon is not None and station_lat is not None:
                            station_lon = station_lon + 360 \
                                if station_lon<0 else station_lon

                            plt.scatter(station_lon, station_lat,
                                        marker='x', color="C2")
                        print("done.")

                        obs_c = get_obs_count(f)
                        obs_count += obs_c

                        print("\tObs Count:", obs_c)

                    converted_obs_count.append(obs_count)

    if exists(TEMP_FILE_PATH):
        remove(TEMP_FILE_PATH)

#    plt.xlim((-180, 180))
#    plt.xlim((0, 360))
#    plt.ylim((-90, 90))
    plt.xlim((85, 215))
    plt.ylim((-65, 20))

    plt.axhline(-60, linestyle='-', color='r')
    plt.axhline(15, linestyle='-', color='r')
    plt.axvline(90, linestyle='-', color='r')
    plt.axvline(210-360, linestyle='-', color='r')
    plt.axvline(210, linestyle='-', color='r')

    plt.xlabel("Longitude (degrees)")
    plt.ylabel("Latitude (degrees)")

    # Dedup labels
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys(), loc='lower right')

    plt.figure()
    plt.plot(dts, converted_obs_count, color="C1", label="Converted")
    plt.plot(dts, prod_obs_count, color="C0", label="Production")

    plt.xlabel("Datetime")
    plt.ylabel("Observation Count")

    plt.legend()

    plt.show()


if __name__ == "__main__":
    main()
