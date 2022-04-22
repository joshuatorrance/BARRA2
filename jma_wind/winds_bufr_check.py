# This script is intended to be used to check the "vital statistics" of a
# winds bufr file to allow comparison between supposedly identical files.
#
# Joshua Torrance

# IMPORTS
from glob import glob
from numpy import array, ndarray, unique
from pandas import concat, to_datetime
from os.path import join, basename, exists
import eccodes as ecc
from sys import path
from matplotlib import pyplot as plt, dates as plt_dates
from datetime import datetime, timedelta, timezone

# Import custom modules
path.insert(1, "/g/data/hd50/jt4085/BARRA2/util/bufr")
from eccodes_wrapper import BufrFile
from jma_interface import get_wind_data

# PARAMETERS

# METHODS
def get_obs_count(filepath, filter_centre_freqs=None):
    with BufrFile(filepath) as bufr:
        if filter_centre_freqs is None:
            obs_count = bufr.get_obs_count()
        else:
            obs_count = 0
            for msg in bufr.get_messages():
                centre_freq = msg.get_value("satelliteChannelCentreFrequency")

                for f in filter_centre_freqs:
                    if abs(f-centre_freq) < 1e9:
                        # This frequency (or close enough) is in the filter
                        obs_count += msg.get_obs_count()

                        break

    return obs_count

def get_locations(filepath, filter_centre_freqs=None):
    latitude = []
    longitude = []
    with BufrFile(filepath) as bufr:
        for msg in bufr.get_messages():
            if filter_centre_freqs is not None:
                centre_freq = msg.get_value("satelliteChannelCentreFrequency")

                for f in filter_centre_freqs:
                    if abs(f-centre_freq) < 1e9:
                        # This frequency (or close enough) is in the filter
                        break
                else:
                    # Skip this message
                    continue

            try:
                lat, lon = msg.get_locations()
            except ValueError:
                # Some BUFR files seems to be corrupted somehow
                continue

            if isinstance(lat, ndarray):
                latitude += lat.tolist()
            else:
                latitude.append(lat)

            if isinstance(lon, ndarray):
                longitude += lon.tolist()
            else:
                longitude.append(lon)

    return array(latitude), array(longitude)


def get_datetimes(filepath):
    datetimes = []
    with BufrFile(filepath) as bufr:
        for msg in bufr.get_messages():
            dts = msg.get_datetimes()

            datetimes += dts

    return array(datetimes)


def get_wind_speed(filepath):
    wind_speeds = []
    with BufrFile(filepath) as bufr:
        for msg in bufr.get_messages():
            ws = msg.get_value("#1#windSpeed")

            if len(ws)>1:
                wind_speeds += ws.tolist()

    return array(wind_speeds)

def get_centre_freqs(filepath):
    central_freqs = []
    with BufrFile(filepath) as bufr:
        for msg in bufr.get_messages():
            central_freq = msg.get_value("satelliteChannelCentreFrequency")

            central_freqs.append(central_freq)

    return array(central_freqs)

# SCRIPT
def main1():
    YEAR = 2015
    MONTH = 6
    DAY = 1
    HOUR = 12
    HOUR = 6
    INPUT_DIR1 = "/scratch/hd50/jt4085/jma_wind/bufr"
    INPUT_FILE_PATH1 = "{in_dir}/{year}/{month:02}/{year}{month:02}{day:02}T{hour:02}00/{year}{month:02}{day:02}T{hour:02}00.bufr"
    #INPUT_FILE_PATH = "/scratch/hd50/jt4085/sonde/data-bufr/ZZXUAICE019-data.bufr"

    INPUT_DIR2 = "/g/data/hd50/barra2/data/obs/production"
    INPUT_FILE_PATH2 = "{in_dir}/{year}/{month:02}/{year}{month:02}{day:02}T{hour:02}00Z/bufr/satwind/mtsat2"

    INPUT_FILE_PATH1 = INPUT_FILE_PATH1.format(in_dir=INPUT_DIR1,
        year=YEAR, month=MONTH, day=DAY, hour=HOUR)
    INPUT_FILE_PATH2 = INPUT_FILE_PATH2.format(in_dir=INPUT_DIR2,
        year=YEAR, month=MONTH, day=DAY, hour=HOUR)

    # Get raw CSV data
    start_dt = datetime(year=YEAR, month=MONTH, day=DAY, hour=HOUR) \
                        - timedelta(hours=3)
    end_dt = datetime(year=YEAR, month=MONTH, day=DAY, hour=HOUR) \
                      + timedelta(hours=3) - timedelta(microseconds=1)
    print("\nStart and end times:")
    print(start_dt)
    print(end_dt)

    SATELLITE_NAMES = ["MTSAT-1R", "MTSAT-2"]
    CHANNEL_NAMES = ["VIS", "IR1", "IR2", "IR3", "IR4"]

    raw_data = None
    for sat in SATELLITE_NAMES:
        for chan in CHANNEL_NAMES:
            raw = get_wind_data(sat, chan, start_dt, end_dt)

            if raw is None:
                continue

            if raw_data is not None:
                for key in raw:
                    raw_data = concat([raw_data, raw[key]], axis=0)
            else:
                for key in raw:
                    if raw_data is not None:
                        raw_data = concat([raw_data, raw[key]], axis=0)
                    else:
                        raw_data = raw[key]

    
    raw_dts = raw_data['time(mjd)']

    raw_wind_speeds = (raw_data["u (m/s)"]**2 + raw_data["v (m/s)"]**2)**0.5
    print("\nRaw DTs")
    print("Range:", raw_dts.max() - raw_dts.min())

    # Get converted data
    print("\nConverted BUFR Data:")
    print(INPUT_FILE_PATH1)
    dts1 = get_datetimes(INPUT_FILE_PATH1)
    wss1 = get_wind_speed(INPUT_FILE_PATH1)

    lat1, lon1 = get_locations(INPUT_FILE_PATH1)

    print("Range: ", dts1.max() - dts1.min())
    print("Obs count for converted file:", get_obs_count(INPUT_FILE_PATH1))

    central_freqs1 = get_centre_freqs(INPUT_FILE_PATH1)
    print("Unique frequencies:", unique(central_freqs1))
    for f in unique(central_freqs1):
        print("\tFreq: {}, Obs Count: {}".format(
            f, get_obs_count(INPUT_FILE_PATH1, [f])))

    print("Converted wind speeds:")
    print("\tMean:", wss1.mean())
    print("\tStd:", wss1.std())
    print("\tMin:", wss1.min())
    print("\tMax:", wss1.max())


    # Get Prod data
    print("\nBUFR Data from production:")
    print(INPUT_FILE_PATH2)
    dts2 = get_datetimes(INPUT_FILE_PATH2)
    wss2 = get_wind_speed(INPUT_FILE_PATH2)

    lat2, lon2 = get_locations(INPUT_FILE_PATH2)

    print("Range: ", dts2.max() - dts2.min())
    print("Obs count for prod file:", get_obs_count(INPUT_FILE_PATH2))

    central_freqs2 = get_centre_freqs(INPUT_FILE_PATH2)
    print("Unique frequencies:", unique(central_freqs2))
    for f in unique(central_freqs2):
        print("\tFreq: {}, Obs Count: {}".format(
            f, get_obs_count(INPUT_FILE_PATH2, [f])))

    print("Production wind speeds:")
    print("\tMean:", wss2.mean())
    print("\tStd:", wss2.std())
    print("\tMin:", wss2.min())
    print("\tMax:", wss2.max())

    # Plot wind_speed and time
    plt.figure(1)
    plt.plot(raw_dts, raw_wind_speeds, ',', color='C0')
    plt.plot(dts1, wss1, ',', color='C1')
    plt.plot(dts2, wss2, ',', color='C2')

    plt.title("JMA Winds")
    plt.xlabel("Datetime")
    plt.ylabel("Wind Speed (m/s)")

    # Plot time against array index
    fig = plt.figure(2)
    ax1 = plt.gca()
    ax2 = ax1.twiny()
    ax1.plot(raw_dts, ',', color='C0')
    ax1.plot(dts1, ',', color='C1')
    ax2.plot(dts2, ',', color='C2')

    plt.title("JMA Winds")
    plt.xlabel("Array Index")
    plt.ylabel("Datetime")

    # Plot dt vs lon
    lon1_offset = lon1.copy()
    for i in range(len(lon1_offset)):
        if lon1_offset[i]<0:
            lon1_offset[i] = lon1_offset[i] + 360

    lon2_offset = lon2.copy()
    for i in range(len(lon2_offset)):
        if lon2_offset[i]<0:
            lon2_offset[i] = lon2_offset[i] + 360
        
    plt.figure(5)

    plt.plot(dts1, lon1_offset, ',', label="New Data", color='C1')
    plt.plot(dts2, lon2_offset, ',', label="Old Data", color='C2')

    plt.legend(labelcolor='linecolor', loc='lower right')

    plt.title("Time vs. Longitude")
    plt.xlabel("Time")
    plt.ylabel("Longitude (degrees)")

    dt_format = plt_dates.DateFormatter("%H:%MZ")
    plt.gca().xaxis.set_major_formatter(dt_format)
    plt.xlim((dts1[0].replace(hour=3, minute=0, second=0),
              dts1[0].replace(hour=9, minute=0, second=0)))

    # Plot dt vs lat
    plt.figure(6)
    plt.plot(dts1, lat1, ',', label="New Data", color='C1')
    plt.plot(dts2, lat2, ',', label="Old Data", color='C2')

    plt.legend(labelcolor='linecolor', loc='lower right')

    plt.title("Time vs Latitude")
    plt.xlabel("Time")
    plt.ylabel("Latitude (degrees)")

    dt_format = plt_dates.DateFormatter("%H:%MZ")
    plt.gca().xaxis.set_major_formatter(dt_format)
    plt.xlim((dts1[0].replace(hour=3, minute=0, second=0),
              dts1[0].replace(hour=9, minute=0, second=0)))

    # Plot lat vs long
    # Plot lat vs long
    plt.figure(3)

    plt.scatter(lon2, lat2)

    # Use prod central freqs to filter converted lat/lon
    lat1_f, lon1_f = get_locations(INPUT_FILE_PATH1,
        filter_centre_freqs=[central_freqs2[0]])

    plt.scatter(lon1_f, lat1_f, marker='x')

    plt.xlabel("Longitude (degrees)")
    plt.ylabel("Latitude (degrees)")

    plt.xlim((-180, 180))
    plt.ylim((-90, 90))

    plt.title("Lon & Lat for {year}/{month:02}/{day:02} {hour:02}:00Z".format(
              year=YEAR, month=MONTH, day=DAY, hour=HOUR))

    # Plot lat vs long with frequency filter
    plt.figure(4)

    plt.scatter(lon2, lat2)

    # Use prod central freqs to filter converted lat/lon
    plt.scatter(lon1_f, lat1_f, marker='x')

    plt.xlabel("Longitude (degrees)")
    plt.ylabel("Latitude (degrees)")

    plt.xlim((-180, 180))
    plt.ylim((-90, 90))

    plt.title("Lon & Lat for {year}/{month:02}/{day:02} {hour:02}:00Z\nFrequency filtered".format(
              year=YEAR, month=MONTH, day=DAY, hour=HOUR))

    plt.show()


def main2():
    converted_dir = "/scratch/hd50/jt4085/jma_wind/bufr"

    prod_dir = "/g/data/hd50/barra2/data/obs/production"
    prod_path = "{in_dir}/{year}/{month}/{dt}Z/bufr/satwind/JMAWINDS_1.bufr"

    datetimes = []
    converted_obs_counts = []
    prod_obs_counts = []

    limit = 1000

    year_dirs = glob(join(converted_dir, "*"))
    year_dirs.sort()
    for year_dir in year_dirs:
        year = basename(year_dir)
        print(year)

        month_dirs = glob(join(year_dir, "*"))
        month_dirs.sort()
        for month_dir in month_dirs:
            month = basename(month_dir)
            print("\t", month)

            dt_dirs = glob(join(month_dir, "*"))
            dt_dirs.sort()
            for dt_dir in dt_dirs:
                dt = basename(dt_dir)
                print("\t\t" + dt)

                date_time = datetime.strptime(dt + "+0000", "%Y%m%dT%H%M%z")

                converted_obs_count = 0
                prod_obs_count = 0


                files = glob(join(dt_dir, "*.bufr"))
                files.sort()
                for f_path in files:
                    f_name = basename(f_path)
                    print("\t\t\t", f_name)

                    print("\t\t\tGetting converted count...", end='')
                    converted_obs_count = get_obs_count(f_path)
                    print("done.")

                # Find the corresponding file in production.
                prod_file_path = prod_path.format(
                    in_dir=prod_dir, year=year, month=month, dt=dt)

                if exists(prod_file_path):
                    print("\t\t\tGetting prod count...", end='')
                    prod_obs_count = get_obs_count(prod_file_path)
                    print("done.")


                datetimes.append(date_time)
                converted_obs_counts.append(converted_obs_count)
                prod_obs_counts.append(prod_obs_count)

                if len(converted_obs_counts)>limit:
                    break
            if len(converted_obs_counts)>limit:
                break
        if len(converted_obs_counts)>limit:
            break

    plt.plot(datetimes, converted_obs_counts, label="Converted")
    plt.plot(datetimes, prod_obs_counts, label="Production")

    plt.ylabel("Number of observations per bin")

    plt.legend()

    plt.show()
                    


if __name__ == "__main__":
    main1()

