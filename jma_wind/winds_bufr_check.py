# This script is intended to be used to check the "vital statistics" of a
# winds bufr file to allow comparison between supposedly identical files.
#
# Joshua Torrance

# IMPORTS
from collections.abc import Iterable
from datetime import datetime, timedelta
from glob import glob
from os.path import join, basename
from sys import path

from cartopy import crs
from matplotlib import cbook, pyplot as plt
from numpy import array, ndarray, unique, concatenate, logical_and, median
from pandas import concat

# Import custom modules
path.insert(1, "/g/data/hd50/jt4085/BARRA2/util/bufr")
from eccodes_wrapper import BufrFile
from jma_interface import get_wind_data

# Silence matplotlib warning related to cartopy
from warnings import filterwarnings
filterwarnings("ignore", category=cbook.mplDeprecation)


# PARAMETERS
BARRA2_CENTRAL_LON = 150


# METHODS
def get_obs_count(filepath, filter_centre_freqs=None):
    with BufrFile(filepath) as bufr:
        if filter_centre_freqs is None:
            obs_count = bufr.get_obs_count()
        else:
            obs_count = 0
            for msg in bufr.get_messages():
                centre_freq = msg.get_value("satelliteChannelCentreFrequency")

                if isinstance(centre_freq, Iterable):
                    centre_freq = unique(centre_freq)
                else:
                    centre_freq = [centre_freq]

                done = False
                for c_f in centre_freq:
                    if done:
                        break

                    for f in filter_centre_freqs:
                        if abs(f-c_f) < 1e9:
                            # This frequency (or close enough) is in the filter
                            obs_count += msg.get_obs_count()

                            done = True
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

            if isinstance(ws, Iterable):
                wind_speeds += ws.tolist()
            else:
                wind_speeds.append(ws)

    return array(wind_speeds)


def get_centre_freqs(filepath):
    central_freqs = []
    with BufrFile(filepath) as bufr:
        for msg in bufr.get_messages():
            central_freq = msg.get_value("satelliteChannelCentreFrequency")

            num_subsets = msg.get_value("numberOfSubsets")

            if not isinstance(central_freq, ndarray):
                central_freq = [central_freq] * num_subsets

            central_freqs = concatenate((central_freqs, central_freq))

    return array(central_freqs)


def plot_lat_lon_markers(lat, lon, marker='x', colour=None):
    ax = plt.axes(projection=crs.PlateCarree(
        central_longitude=BARRA2_CENTRAL_LON))
    ax.coastlines()

    plt.plot(lon, lat, marker, color=colour, transform=crs.PlateCarree())

    plt.xlabel("Longitude (degrees)")
    plt.ylabel("Latitude(degrees)")


# SCRIPT
def main1():
    YEAR = 1995
    MONTH = 6
    DAY = 13
    HOUR = 12
    INPUT_DIR1 = "/scratch/hd50/jt4085/jma_wind/bufr"
    INPUT_FILE_PATH1 = "{in_dir}/{year}/{month:02}/{year}{month:02}{day:02}T{hour:02}00/{year}{month:02}{day:02}T{hour:02}00.bufr"
    #INPUT_FILE_PATH = "/scratch/hd50/jt4085/sonde/data-bufr/ZZXUAICE019-data.bufr"

    INPUT_DIR2 = "/g/data/hd50/barra2/data/obs/production"
    INPUT_FILE_PATH2 = "{in_dir}/{year}/{month:02}/{year}{month:02}{day:02}T{hour:02}00Z/bufr/satwind/JMAWINDS_1.bufr"

    INPUT_FILE_PATH1 = INPUT_FILE_PATH1.format(in_dir=INPUT_DIR1, year=YEAR,
                                               month=MONTH, day=DAY, hour=HOUR)
    INPUT_FILE_PATH2 = INPUT_FILE_PATH2.format(in_dir=INPUT_DIR2, year=YEAR,
                                               month=MONTH, day=DAY, hour=HOUR)

    start_dt = datetime(year=YEAR, month=MONTH, day=DAY, hour=HOUR) \
                        - timedelta(hours=3)
    end_dt = datetime(year=YEAR, month=MONTH, day=DAY, hour=HOUR) \
                      + timedelta(hours=3) - timedelta(microseconds=1)

    print("\nStart and end times:")
    print(start_dt)
    print(end_dt)

    # Get raw CSV data
    if True:
        SATELLITE_NAMES = ["GMS-5", "MTSAT-1R", "MTSAT-2", "GOES-9"]
        CHANNEL_NAMES = ["VIS", "IR1", "IR2", "IR3", "IR4"]
        CHANNEL_NAMES = ["VIS", "IR1", "IR3"]

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
    plt.figure(2)
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
        if lon1_offset[i] < 0:
            lon1_offset[i] = lon1_offset[i] + 360

    lon2_offset = lon2.copy()
    for i in range(len(lon2_offset)):
        if lon2_offset[i] < 0:
            lon2_offset[i] = lon2_offset[i] + 360
        
    plt.figure(5)

    plt.plot(dts1, lon1_offset, ',', label="New Data", color='C1')
    plt.plot(dts2, lon2_offset, ',', label="Old Data", color='C2')

    plt.legend(labelcolor='linecolor', loc='lower right')

    plt.title("Time vs. Longitude")
    plt.xlabel("Time")
    plt.ylabel("Longitude (degrees)")

#    dt_format = plt_dates.DateFormatter("%H:%MZ")
#    plt.gca().xaxis.set_major_formatter(dt_format)
#    plt.xlim((dts1[0].replace(hour=3, minute=0, second=0),
#              dts1[0].replace(hour=9, minute=0, second=0)))

    # Plot dt vs lat
    plt.figure(6)
    plt.plot(dts1, lat1, ',', label="New Data", color='C1')
    plt.plot(dts2, lat2, ',', label="Old Data", color='C2')

    plt.legend(labelcolor='linecolor', loc='lower right')

    plt.title("Time vs Latitude")
    plt.xlabel("Time")
    plt.ylabel("Latitude (degrees)")

#    dt_format = plt_dates.DateFormatter("%H:%MZ")
#    plt.gca().xaxis.set_major_formatter(dt_format)
#    plt.xlim((dts1[0].replace(hour=3, minute=0, second=0),
#              dts1[0].replace(hour=9, minute=0, second=0)))

    # Plot lat vs lon
    plt.figure(3)

    plt.scatter(lon1, lat1, marker='+', label='New')
    plt.scatter(lon2, lat2, marker='x', label='Old')

    # Use prod central freqs to filter converted lat/lon
    lat1_f, lon1_f = get_locations(INPUT_FILE_PATH1,
                                   filter_centre_freqs=[central_freqs2[0]])

#    plt.scatter(lon1_f, lat1_f, marker='x')

    plt.xlabel("Longitude (degrees)")
    plt.ylabel("Latitude (degrees)")

    plt.xlim((-180, 180))
    plt.ylim((-90, 90))

    plt.title("Lon & Lat for {year}/{month:02}/{day:02} {hour:02}:00Z".format(
              year=YEAR, month=MONTH, day=DAY, hour=HOUR))

    plt.legend()

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


    # Examine a smaller area
    # Filter to one freq channel
    freq_channel = central_freqs2[0]

    indices1 = abs(central_freqs1-freq_channel) < 1e9
    indices2 = abs(central_freqs2-freq_channel) < 1e9

    lon1_filtered = lon1[indices1]
    lat1_filtered = lat1[indices1]
    wss1_filtered = wss1[indices1]

    lon2_filtered = lon2[indices2]
    lat2_filtered = lat2[indices2]
    wss2_filtered = wss2[indices2]

    # Filter to a small geographic region - say around Melbourne
    geo_centre_lon = 144.96
    geo_centre_lat = -37.81
    geo_width = 40
    # Further zoomed in on a dense-ish patch.
    geo_centre_lon = 135.01
    geo_centre_lat = -40.01
    geo_width = 10

    geo_left = geo_centre_lon - geo_width/2
    geo_right = geo_centre_lon + geo_width/2
    geo_bottom = geo_centre_lat - geo_width/2
    geo_top = geo_centre_lat + geo_width/2

    indices1 = logical_and(
                   logical_and((geo_left < lon1_filtered),
                               (lon1_filtered < geo_right)),
                   logical_and((geo_bottom < lat1_filtered),
                               (lat1_filtered < geo_top)))
    indices2 = logical_and(
                   logical_and((geo_left < lon2_filtered),
                               (lon2_filtered < geo_right)),
                   logical_and((geo_bottom < lat2_filtered),
                               (lat2_filtered < geo_top)))

    lon1_filtered = lon1_filtered[indices1]
    lat1_filtered = lat1_filtered[indices1]
    wss1_filtered = wss1_filtered[indices1]

    lon2_filtered = lon2_filtered[indices2]
    lat2_filtered = lat2_filtered[indices2]
    wss2_filtered = wss2_filtered[indices2]

    plt.figure()

    plt.scatter(lon1_filtered, lat1_filtered, marker='x', label='New')
    plt.scatter(lon2_filtered, lat2_filtered, marker='+', label='Old')

    plt.title("Single Freq Channel, Geo filtered, Lon vs Lat")
    plt.xlabel("Longitude (degrees)")
    plt.ylabel("Latitude (degrees)")


    if False:
        print("\n\n")
        print("######################################")
        print("Single Channel: {:e} Hz\n".format(central_freqs1[0]))

        print("Small geographic area\n")
        print("New Converted Data:")
        print("Wind speed:")
        print("\tNumber of obs:", len(wss1_filtered))
        print("\tMean:",          wss1_filtered.mean())
        print("\tMedian:",        median(wss1_filtered))
        print("\tStd:",           wss1_filtered.std())
        print("\tMin:",           wss1_filtered.min())
        print("\tMax:",           wss1_filtered.max())

        print()

        print("Old Data in Production:")
        print("Wind speed:")
        print("\tNumber of obs:", len(wss2_filtered))
        print("\tMean:",          wss2_filtered.mean())
        print("\tMedian:",        median(wss2_filtered))
        print("\tStd:",           wss2_filtered.std())
        print("\tMin:",           wss2_filtered.min())
        print("\tMax:",           wss2_filtered.max())

    # Narrow vertical slice
    slice_lon =  157
    slice_width = 0.5
    # Further Zoomed in
    slice_lon =  135
    slice_width = 0.2

    indices1 = logical_and(
                   (slice_lon - slice_width < lon1_filtered),
                   (lon1_filtered < slice_lon + slice_width))
    indices2 = logical_and(
                   (slice_lon - slice_width < lon2_filtered),
                   (lon2_filtered < slice_lon + slice_width))

    lon1_filtered = lon1_filtered[indices1]
    lat1_filtered = lat1_filtered[indices1]
    wss1_filtered = wss1_filtered[indices1]

    lon2_filtered = lon2_filtered[indices2]
    lat2_filtered = lat2_filtered[indices2]
    wss2_filtered = wss2_filtered[indices2]

    plt.scatter(lon1_filtered, lat1_filtered,
                color='cyan', marker='x', label='New - Slice')
    plt.scatter(lon2_filtered, lat2_filtered,
                color='r', marker='+', label='Old - Slice')

    plt.legend()

    if False:
        print("\n\n")
        print("######################################")
        print("Single Channel: {:e} Hz\n".format(central_freqs1[0]))

        print("Small geographic Slice\n")
        print("New Converted Data:")
        print("Wind speed:")
        print("\tNumber of obs:", len(wss1_filtered))
        print("\tMean:",          wss1_filtered.mean())
        print("\tMedian:",        median(wss1_filtered))
        print("\tStd:",           wss1_filtered.std())
        print("\tMin:",           wss1_filtered.min())
        print("\tMax:",           wss1_filtered.max())

        print()

        print("Old Data in Production:")
        print("Wind speed:")
        print("\tNumber of obs:", len(wss2_filtered))
        print("\tMean:",          wss2_filtered.mean())
        print("\tMedian:",        median(wss2_filtered))
        print("\tStd:",           wss2_filtered.std())
        print("\tMin:",           wss2_filtered.min())
        print("\tMax:",           wss2_filtered.max())

    plt.show()


def main2():
    converted_dir = "/scratch/hd50/jt4085/jma_wind/bufr"

    prod_dir = "/g/data/hd50/barra2/data/obs/production"
    prod_path = "{in_dir}/{year}/{month}/{dt}Z/bufr/satwind/JMAWINDS_*.bufr"

    datetimes = []
    converted_obs_counts = []
    prod_obs_counts = []

    year_dirs = glob(join(converted_dir, "*"))
    year_dirs.sort()
    for year_dir in year_dirs:
        year = basename(year_dir)
        print(year)

        if int(year)<1990:
            print("\tBefore GMS-4, skipping")
            continue
        elif int(year)>1995:
            print("\tAfter GMS-4, skipping")
            continue
#        if int(year)<1990:
#            print("\tBefore GMS-4, skipping")
#            continue

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

                if False and dt<"19950611T1200":
                    print("\t\t\tAlready done, skipping.")
                    continue
                elif True and dt>"19950613T0600":
                    print("\t\t\tAlready done, skipping.")
                    continue

                date_time = datetime.strptime(dt + "+0000", "%Y%m%dT%H%M%z")

                converted_obs_count = 0
                prod_obs_count = 0

                # Find the converted/moved/new files
                files = glob(join(dt_dir, "*.bufr"))
                files.sort()
                for f_path in files:
                    f_name = basename(f_path)
                    print("\t\t\t" + f_name)

                    print("\t\t\tGetting converted count...", end='')
                    count = get_obs_count(f_path)
                    converted_obs_count += count
                    print("done.")

                    if False:
                        print("\t\t\tGetting converted lat/lon...", end='')
                        lat, lon = get_locations(f_path)
                        print("done.")

                        plot_lat_lon_markers(lat, lon, marker='+')

                    print()

                # Find the corresponding file/s in production.
                prod_file_path = prod_path.format(
                    in_dir=prod_dir, year=year, month=month, dt=dt)

                prod_filepaths = glob(prod_file_path)

                for prod_file_path in prod_filepaths:
                    print("\t\t\tGetting prod count...", end='')
                    prod_obs_count = get_obs_count(prod_file_path)
                    print("done.")

                    if False:
                        print("\t\t\tGetting prod lat/lon...", end='')
                        lat, lon = get_locations(prod_file_path)
                        print("done.")

                        plot_lat_lon_markers(lat, lon, marker='x')
                    
                datetimes.append(date_time)
                converted_obs_counts.append(converted_obs_count)
                prod_obs_counts.append(prod_obs_count)


    plt.figure()

    plt.plot(datetimes, converted_obs_counts, '-x', label="Converted")
    plt.plot(datetimes, prod_obs_counts, '-+', label="Production")

    plt.ylabel("Number of observations per bin")
    plt.xlabel("Datetime")

    plt.legend()

    plt.show()


if __name__ == "__main__":
    main2()
