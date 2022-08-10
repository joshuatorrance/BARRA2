# Tested with the following module:
# module load python3/3.8.5
# module load eccodes3
#
# Doesn't seem to work with analysis3 (eccodes? local table def for Vincent's bufrs?):
# module load conda/analysis3-22.04

# IMPORTS
from collections.abc import Iterable
from datetime import datetime, timezone, timedelta
from os.path import join, exists, getsize, basename
from sys import path
from glob import glob
from tempfile import TemporaryDirectory
from tarfile import open as open_tar
from xml.etree.ElementInclude import include
from pandas import concat as concat_dataframe, date_range, DataFrame, Series, read_csv
from numpy import full, datetime64, unique
from matplotlib import pyplot as plt, use

#from cartopy import crs

import eccodes as ecc

# Import custom modules
path.insert(1, "/g/data/hd50/jt4085/BARRA2/util/bufr")
from eccodes_wrapper import BufrFile

# conda/analysis3 modules wants to use QT for matplotlib, use TKAgg instead
#use('TKAgg')

# PARAMETERS
# C3
C3_NAME = "C3"
DATA_DIR_C3 = "/g/data/ig2/ACCESS_prod/access_c3/access_c3_getobs"
PATH_TEMPLATE_C3 = "{year:04d}/{month:02d}/{year:04d}{month:02d}{day:02d}T{hour:02d}00Z/{year:04d}{month:02d}{day:02d}T{hour:02d}00Z_obs.tar.gz"
PATH_C3 = join(DATA_DIR_C3, PATH_TEMPLATE_C3)

TARBALL_STRUCTURE_C3 = "bufr/satwind/GOESBUFR_*.bufr"

# Vincent
VINC_NAME = "Vinc"
DATA_DIR_VINC = "/g/data/ig2/SATWIND/nwcsaf_winds_experiment"
PATH_TEMPLATE_VINC = "{month:02d}/S_NWC_HRW-WINDIWWG_HIMA08_HIMA-N-BS_{year:04d}{month:02d}{day:02d}T{hour:02d}{minute}00Z.bufr"
PATH_VINC = join(DATA_DIR_VINC, PATH_TEMPLATE_VINC)
#PATH_VINC = "/home/548/jt4085/testing/amvs_for_fiona/AMV_*.bufr"

# Cycle details
CYCLE_DT_FORMAT = "%Y%m%dT%H%MZ"
CYCLE_WIDTH = timedelta(hours=6)

# Dataframe
COLUMN_NAMES_TYPES = {"datetime": 'datetime64[ns]',
                      "pressure": float,
                      "longitude": float,
                      "latitude": float,
                      "wind_speed": float,
                      "wind_direction": float,
                      "channel centre frequency": float}

# Path to save dataframes to save re-loading from bufrs
DATAFRAME_DIR = "/scratch/hd50/jt4085/pandas_dataframes"
DATAFRAME_FILENAME_TEMPLATE = "{source}-{year:04d}{month:02d}{day:02d}T{hour:02d}00Z.csv"
DATAFRAME_PATH = join(DATAFRAME_DIR, DATAFRAME_FILENAME_TEMPLATE)


# METHODS
def round_off_datetime_to_6hour(datetime):
    unix_time = datetime.timestamp()

    seconds_per_6_hours = 6*60*60

    rounded_time = (unix_time // seconds_per_6_hours) * seconds_per_6_hours

    return datetime.fromtimestamp(rounded_time, timezone.utc)


def get_data_for_cycle_c3(cycle_time, save_to_file=True):
    print(f"Getting C3 data for {cycle_time}")

    dataframe_filepath = DATAFRAME_PATH.format(source=C3_NAME, year=cycle_time.year,
                                               month=cycle_time.month, day=cycle_time.day,
                                               hour=cycle_time.hour)

    if exists(dataframe_filepath):
        df = load_dataframe_from_csv(dataframe_filepath)
    else:
        earliest_dt = cycle_time - CYCLE_WIDTH / 2
        latest_dt = cycle_time + CYCLE_WIDTH / 2

        # C3 data can be up to hourly but there's sometime missing data
        # So build a date range and find files that match any of those datetimes
        datetimes = date_range(earliest_dt, latest_dt, freq="1H",
                               #closed='right')
                               inclusive='left')

        # Get all the tarballs in the 6 hour window
        tarball_paths = []
        for dt in datetimes:
            path = PATH_C3.format(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour)

            tarball_paths.append(path)

        if len(tarball_paths)==0:
            raise IOError(f"No data found in C3 for {cycle_time}.")

        # Unpack each tarball into a temporary directory
        dfs = []
        for tarball_path in tarball_paths:
            with TemporaryDirectory() as temp_dir, open_tar(tarball_path) as tarball:
                print(f"    Extracting tarball", basename(tarball_path))
                tarball.extractall(path=temp_dir)

                # Grab the data of interest from each bufr file
                bufr_paths = glob(join(temp_dir, TARBALL_STRUCTURE_C3))
                tarball_df = get_data_from_bufrs_c3(bufr_paths)

                dfs.append(tarball_df)

        df = concat_dataframe(dfs)

        if save_to_file:
            df.to_csv(dataframe_filepath, index=False)

    return df


def get_data_for_cycle_vinc(cycle_time, save_to_file=True):
    print(f"Getting Vincent's data for {cycle_time}")

    dataframe_filepath = DATAFRAME_PATH.format(source=VINC_NAME, year=cycle_time.year,
                                               month=cycle_time.month, day=cycle_time.day,
                                               hour=cycle_time.hour)

    if exists(dataframe_filepath):
        df = load_dataframe_from_csv(dataframe_filepath)
    else:
        earliest_dt = cycle_time - CYCLE_WIDTH / 2
        latest_dt = cycle_time + CYCLE_WIDTH / 2

        # Vincent's data seems to be every 10 minutes.
        # It also is only over a few months of 2020 - 02, 03, 04
        # Make an hourly date range and set minutes to *
        datetimes = date_range(earliest_dt, latest_dt, freq="1H",
                               #closed='right')
                               inclusive='left')

        # Get all the files in the 6 hour window
        bufr_paths = []
        for dt in datetimes:
            path = PATH_VINC.format(year=dt.year, month=dt.month, day=dt.day,
                                    hour=dt.hour, minute="*")

            paths = glob(path)
            paths.sort()

            bufr_paths += paths

         # Load each bufr and add it to the dataframe
        df = get_data_from_bufrs_vinc(bufr_paths, compressed=False)

        if save_to_file:
            df.to_csv(dataframe_filepath, index=False)

    return df


def get_data_for_cycle(cycle_time, save_to_file=True):
    if isinstance(cycle_time, datetime):
        pass
    elif isinstance(cycle_time, str):
        # Replace Zs with +0000 and %z so datetime has UTC timezone
        cycle_time = datetime.strptime(cycle_time.replace('Z', '+0000'),
                                       CYCLE_DT_FORMAT.replace('Z', '%z'))
    else:
        print("get_data_for_cycle: cycle_time should be a datetime or a string - yyyymmddThhmmZ")

    # Round off the time to the nearest 6 hours (i.e. hour = 0, 6, 12 or 18)
    cycle_time = round_off_datetime_to_6hour(cycle_time)

    # Grab the data from the two sources
    df_c3 = get_data_for_cycle_c3(cycle_time)
    df_vinc = get_data_for_cycle_vinc(cycle_time)

    return df_c3, df_vinc


def get_data_from_bufrs_c3(bufr_paths, pres_wind_pre_str="#1#", compressed=False):
    # Create an empty dataframe specifying the column name and type
    df = DataFrame({col_name: Series(dtype=col_type)
                    for col_name, col_type in COLUMN_NAMES_TYPES.items()})

    for bufr_path in bufr_paths:
        print(f"        Getting data from", basename(bufr_path))

        with BufrFile(bufr_path, compressed=compressed) as bufr:
            for bufr_msg in bufr.get_messages():
                datetimes = bufr_msg.get_datetimes()
                #lats, lons = bufr_msg.get_locations()
                lats = bufr_msg.get_value(pres_wind_pre_str + "latitude")
                lons = bufr_msg.get_value(pres_wind_pre_str + "longitude")
                centre_freqs = bufr_msg.get_value("satelliteChannelCentreFrequency")

                # C3 data's bufr sequence wants a #1# infront of these
                pressures = bufr_msg.get_value(pres_wind_pre_str + "pressure")
                wind_speeds = bufr_msg.get_value(pres_wind_pre_str + "windSpeed")
                wind_dirs = bufr_msg.get_value(pres_wind_pre_str + "windDirection")

                # There are 5 wind_speed and wind_dir per subset, #1# only gives us only 1

                # variables are sometimes a single value per message
                #  duplicate them into an array
                if not isinstance(centre_freqs, Iterable):
                    centre_freqs = full(datetimes.shape, centre_freqs)

                small_df = DataFrame(zip(datetimes, pressures, lons, lats,
                                         wind_speeds, wind_dirs, centre_freqs),
                                     columns=COLUMN_NAMES_TYPES.keys())

                df = concat_dataframe([df, small_df])

    return df


def get_data_from_bufrs_vinc(bufr_paths, compressed=False):
    # Create an empty dataframe specifying the column name and type
    df = DataFrame({col_name: Series(dtype=col_type)
                    for col_name, col_type in COLUMN_NAMES_TYPES.items()})

    for bufr_path in bufr_paths:
        print(f"        Getting data from", basename(bufr_path))

        with BufrFile(bufr_path, compressed=compressed) as bufr:
            for bufr_msg in bufr.get_messages():
                print("*************")
                
                num_subsets = bufr_msg.get_value("numberOfSubsets")
                print("Num subsets:", num_subsets)

                datetimes = bufr_msg.get_datetimes()

                print("Datetimes len:", len(datetimes))

                # There are two lat/lon for each subset
                lats, lons = bufr_msg.get_locations()
                print("Lons len:", len(lons))
                print("Lats len:", len(lats))
                lats = lats[::2]
                lons = lons[::2]

                # There are three centre_freqs per subset
                centre_freqs = bufr_msg.get_value("satelliteChannelCentreFrequency")
                print("Centre freqs len:", len(centre_freqs))
                centre_freqs = centre_freqs[::3]

                # There are 8 pressures per subset
                pressures = bufr_msg.get_value("pressure")
                print("Pressures len:", len(pressures))
                pressures = pressures[::8]

                # Only one wind speed and dir per subset
                wind_speeds = bufr_msg.get_value("windSpeed")
                wind_dirs = bufr_msg.get_value("windDirection")

                print("Wind speed len:", len(wind_speeds))
                print("Wind dirs len:", len(wind_dirs))

                # variables are sometimes a single value per message
                #  duplicate them into an array
                if not isinstance(centre_freqs, Iterable):
                    centre_freqs = full(datetimes.shape, centre_freqs)

                if not isinstance(wind_speeds, Iterable):
                    wind_speeds = full(datetimes.shape, wind_speeds)

                if not isinstance(wind_dirs, Iterable):
                    wind_dirs = full(datetimes.shape, wind_dirs)

                small_df = DataFrame(zip(datetimes, pressures, lons, lats,
                                         wind_speeds, wind_dirs, centre_freqs),
                                     columns=COLUMN_NAMES_TYPES.keys())

                df = concat_dataframe([df, small_df])

    return df


def load_dataframe_from_csv(csv_path):
    print("\tLoading data from saved CSV", basename(csv_path))
    df = read_csv(csv_path,
                  dtype={k: COLUMN_NAMES_TYPES[k] for k in COLUMN_NAMES_TYPES if k!="datetime"},
                  parse_dates=["datetime"])

    return df


# MAIN
def main():
    df_c3, df_vinc = get_data_for_cycle("20200201T1800Z")

    print("C3 Dataframe:")
    df_c3.info()
    print()
    print("Vinc Dataframe:")
    df_vinc.info()
    print()

    # Obs Counts
    print("Number of obs in C3 dataframe: {:,d}".format(df_c3.size))
    print("Number of obs in Vinc dataframe: {:,d}".format(df_vinc.size))
    print()

    # Filter the dataframes to look at a single time only
    count = 0
    target = 0
    filter_time = None
    for c3_time in df_c3["datetime"].unique()[::-1]:
        for vinc_time in df_vinc["datetime"].unique():
            if c3_time == vinc_time:
                if count >= target:
                    filter_time = c3_time

                    break
                else:
                    count += 1

        if filter_time:
            break

    print("Filtering dataframe - datetime =", filter_time)
    df_c3 = df_c3[filter_time == df_c3['datetime']]
    df_vinc = df_vinc[filter_time == df_vinc['datetime']]

    # Channel Frequencies
    for df, name in [(df_c3, "C3"), (df_vinc, "Vinc")]:
        print(f"Available Frequencies/Wavelenths for {name}:")

        unique_fs = df["channel centre frequency"].unique()
        unique_fs.sort()
        for f in unique_fs:
            print("{:.2f} THz / {:.2f} um".format(f*1e-12, 1e6 * 3e8 / f))

        print()

    # Let's filter on a 2 similar freq channels
    target_f_c3 = 2.72727e13
    target_f_vinc = 2.680e+13

    df_c3 = df_c3[
        (0.98*target_f_c3 < df_c3['channel centre frequency']) &
        (df_c3['channel centre frequency'] < 1.02*target_f_c3)
    ]

    df_vinc = df_vinc[
        (0.975*target_f_vinc < df_vinc['channel centre frequency']) &
        (df_vinc['channel centre frequency'] < 1.025*target_f_vinc)
    ]

    # Filter on pressure level range
    p_min, p_max = 20000, 30000
    
    def filter_df_for_pressure(df, pmin, pmax):
        return df[(pmin < df['pressure']) & (df['pressure'] < pmax)]

    # Build a list of counts at pressure levels
    pmins = []
    c3_pressure_counts = []
    vinc_pressure_counts = []
    d_pressure = 10000
    for pmin in range(10000, 100000, d_pressure):
        pmins.append(pmin)

        c3_len = len(filter_df_for_pressure(df_c3, pmin, pmin+d_pressure))
        vinc_len = len(filter_df_for_pressure(df_vinc, pmin, pmin+d_pressure))

        c3_pressure_counts.append(c3_len)
        vinc_pressure_counts.append(vinc_len)

    # Now actually filter
    print(f"Filtering pressure from {p_min} to {p_max} hPa.")
    print()

    df_c3 = df_c3[
        (p_min < df_c3['pressure']) &
        (df_c3['pressure'] < p_max)
    ]

    df_vinc = df_vinc[
        (p_min < df_vinc['pressure']) &
        (df_vinc['pressure'] < p_max)
    ]

    print("Number of filtered obs in C3 dataframe: {:,d}".format(df_c3.size))
    print("Number of filtered obs in Vinc dataframe: {:,d}".format(df_vinc.size))
    print()

    do_zoom = False
    if do_zoom:
        # Grab a subset for closer examination
        vinc_mean_lon = df_vinc['longitude'].mean()
        vinc_mean_lat = df_vinc['latitude'].mean()
        vinc_std_lon = df_vinc['longitude'].std()
        vinc_std_lat = df_vinc['latitude'].std()

        # Filter on the zoomed subset
        f = 0.25
        df_vinc_filtered = df_vinc[
            ((vinc_mean_lon - f*vinc_std_lon) < df_vinc.longitude) &
            (df_vinc.longitude < (vinc_mean_lon + f*vinc_std_lon)) &
            ((vinc_mean_lat - f*vinc_std_lat) < df_vinc.latitude) &
            (df_vinc.latitude < (vinc_mean_lat + f*vinc_std_lat))
        ]

        df_c3_filtered = df_c3[
            ((vinc_mean_lon - f*vinc_std_lon) < df_c3.longitude) &
            (df_c3.longitude < (vinc_mean_lon + f*vinc_std_lon)) &
            ((vinc_mean_lat - f*vinc_std_lat) < df_c3.latitude) &
            (df_c3.latitude < (vinc_mean_lat + f*vinc_std_lat))
        ]

    # Plot lat/lon for the full channel
    plt.figure()
    centre_lon = 133
    proj = crs.PlateCarree(central_longitude=centre_lon)
    ax = plt.axes(projection=proj)
    ax.coastlines()

    plt.title("Lat/Lon for {:.2f} THz Channel".format(target_f_c3*1e-12))
    ax.plot(df_c3['longitude'], df_c3['latitude'], '+', color='r', label='C3', transform=crs.PlateCarree())
    ax.plot(df_vinc['longitude'], df_vinc['latitude'], 'x', color='b', label="Vincent's", transform=crs.PlateCarree())

    if do_zoom:
        plt.plot(df_vinc['longitude'], df_vinc['latitude'], 'x', color='g', label="Vincent's (filtered)")

    plt.legend(loc='lower left')

    plt.xlabel("Longitude (degrees)")
    plt.ylabel("Latitude (degrees)")

    if do_zoom:
        # Plot lat/lon for the subset
        plt.figure()
        plt.title("Zoomed in Lat/Lon for {:.2f} THz Channel".format(target_f_c3*1e-12))
        plt.plot(df_vinc_filtered['longitude'], df_vinc_filtered['latitude'], 'x', color='b', label="Vincent's")
        plt.plot(df_c3_filtered['longitude'], df_c3_filtered['latitude'], '+', color='r', label='C3')

        plt.legend(loc='lower left')

        plt.xlabel("Longitude (degrees)")
        plt.ylabel("Latitude (degrees)")

    # Plot a histogram of wind speed and of wind dir for subsets
    plt.figure()

    # Vinc's wind speed histogram
    plt.subplot(2, 2, 1)
    plt.title("Vinc")

    if do_zoom:
        wind_speed = df_vinc_filtered["wind_speed"]
    else:
        wind_speed = df_vinc["wind_speed"]
    plt.hist(wind_speed, 100, label="Wind Speed - Vincent's", color='b')
    s = "Mean: {:.2f}\nStd: {:.2f}".format(wind_speed.mean(), wind_speed.std())
    plt.annotate(s, (0, 0), (0.65, 0.8), xycoords="axes fraction")

    plt.xlabel("Wind Speed (m/s)")
    plt.ylabel("Occurance")

    # C3's wind speed histogram
    plt.subplot(2, 2, 2)
    plt.title("C3")
    
    if do_zoom:
        wind_speed = df_c3_filtered["wind_speed"]
    else:
        wind_speed = df_c3["wind_speed"]
    plt.hist(wind_speed, 100, label="Wind Speed - C3", color='r')
    
    s = "Mean: {:.2f}\nStd: {:.2f}".format(wind_speed.mean(), wind_speed.std())
    plt.annotate(s, (0, 0), (0.65, 0.8), xycoords="axes fraction")

    plt.xlabel("Wind Speed (m/s)")

    # Vinc's wind dir histogram
    plt.subplot(2, 2, 3)
    if do_zoom:
        wind_dir = df_vinc_filtered["wind_direction"]
    else:
        wind_dir = df_vinc["wind_direction"]
    plt.hist(wind_dir, 100, label="Wind Direction - Vincent's", color='b')
    
    s = "Mean: {:.2f}\nStd: {:.2f}".format(wind_dir.mean(), wind_dir.std())
    plt.annotate(s, (0, 0), (0.55, 0.8), xycoords="axes fraction")

    plt.xlabel("Wind Direction (degrees)")
    plt.ylabel("Occurance")

    # C3's wind dir histogram
    plt.subplot(2, 2, 4)
    if do_zoom:
        wind_dir = df_c3_filtered["wind_direction"]
    else:
        wind_dir = df_c3["wind_direction"]
    plt.hist(wind_dir, 100, label="Wind Direction - C3", color='r')
    
    s = "Mean: {:.2f}\nStd: {:.2f}".format(wind_dir.mean(), wind_dir.std())
    plt.annotate(s, (0, 0), (0.55, 0.8), xycoords="axes fraction")

    plt.xlabel("Wind Direction (degrees)")

    # Temporal profile
    plt.figure()

    plt.title("Time vs. Latitude")
    plt.plot(df_c3['datetime'], df_c3['latitude'], '+', color='r', label='C3')
    plt.plot(df_vinc['datetime'], df_vinc['latitude'], 'x', color='b', label="Vincent's")

    plt.xlabel("Time (datetime)")
    plt.ylabel("Latitude (degrees)")

    plt.legend(loc='lower left')

    # Show the pressure level counts
    plt.figure()

    plt.title("Obs Counts at Pressure levels")

    plt.plot(pmins, c3_pressure_counts, color='r', label='C3')
    plt.plot(pmins, vinc_pressure_counts, color='b', label="Vincent's")

    plt.axhline(0, color='k', linestyle='--')
    plt.xlabel("Pressure (hPa)")
    plt.ylabel("Obs Count")

    plt.legend(loc='lower left')

    plt.show()


if __name__ == "__main__":
    main()
