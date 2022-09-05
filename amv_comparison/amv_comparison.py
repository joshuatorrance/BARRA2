# Tested with the following module:
# module load python3/3.8.5
# module load eccodes3
#
# Doesn't seem to work with analysis3 (eccodes? local table def for NWCSAF bufrs?):
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
import matplotlib.gridspec as gridspec

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

# NWCSAF
NWCSAF_NAME = "NWCSAF"
DATA_DIR_NWCSAF = "/g/data/ig2/SATWIND/nwcsaf_winds_experiment"
PATH_TEMPLATE_NWCSAF = "{month:02d}/S_NWC_HRW-WINDIWWG_HIMA08_HIMA-N-BS_{year:04d}{month:02d}{day:02d}T{hour:02d}{minute}00Z.bufr"
PATH_NWCSAF = join(DATA_DIR_NWCSAF, PATH_TEMPLATE_NWCSAF)
#PATH_NWCSAF = "/home/548/jt4085/testing/amvs_for_fiona/AMV_*.bufr"

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
                      "channel centre frequency": float,
                      "quality indicator": float}

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


def get_data_for_cycle_nwcsaf(cycle_time, save_to_file=True):
    print(f"Getting NWCSAF data for {cycle_time}")

    dataframe_filepath = DATAFRAME_PATH.format(source=NWCSAF_NAME, year=cycle_time.year,
                                               month=cycle_time.month, day=cycle_time.day,
                                               hour=cycle_time.hour)

    if exists(dataframe_filepath):
        df = load_dataframe_from_csv(dataframe_filepath)
    else:
        earliest_dt = cycle_time - CYCLE_WIDTH / 2
        latest_dt = cycle_time + CYCLE_WIDTH / 2

        # NWCSAF data seems to be every 10 minutes.
        # It also is only over a few months of 2020 - 02, 03, 04
        # Make an hourly date range and set minutes to *
        datetimes = date_range(earliest_dt, latest_dt, freq="1H",
                               #closed='right')
                               inclusive='left')

        # Get all the files in the 6 hour window
        bufr_paths = []
        for dt in datetimes:
            path = PATH_NWCSAF.format(year=dt.year, month=dt.month, day=dt.day,
                                    hour=dt.hour, minute="*")

            paths = glob(path)
            paths.sort()

            bufr_paths += paths

         # Load each bufr and add it to the dataframe
        df = get_data_from_bufrs_nwcsaf(bufr_paths, compressed=False)

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
    df_nwcsaf = get_data_for_cycle_nwcsaf(cycle_time)

    return df_c3, df_nwcsaf


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
                qis = bufr_msg.get_value(pres_wind_pre_str + "windSpeed->percentConfidence")

                # There are 5 wind_speed and wind_dir per subset, #1# only gives us only 1
                # I don't know which percentConfidence represents the QI, this is pure guess work

                # variables are sometimes a single value per message
                #  duplicate them into an array
                if not isinstance(centre_freqs, Iterable):
                    centre_freqs = full(datetimes.shape, centre_freqs)

                if not isinstance(lons, Iterable):
                    lons = full(datetimes.shape, lons)

                if not isinstance(lats, Iterable):
                    lats = full(datetimes.shape, lats)

                if not isinstance(qis, Iterable):
                    qis = full(datetimes.shape, qis)

                small_df = DataFrame(zip(datetimes, pressures, lons, lats,
                                         wind_speeds, wind_dirs, centre_freqs, qis),
                                         columns=COLUMN_NAMES_TYPES.keys())

                df = concat_dataframe([df, small_df])

    return df


def get_data_from_bufrs_nwcsaf(bufr_paths, compressed=False):
    # Create an empty dataframe specifying the column name and type
    df = DataFrame({col_name: Series(dtype=col_type)
                    for col_name, col_type in COLUMN_NAMES_TYPES.items()})

    for bufr_path in bufr_paths:
        print(f"        Getting data from", basename(bufr_path))

        # Correct offset handling copied from Vincent's code at:
        # https://git.nci.org.au/vov548/iwwg_bufr_io/-/blob/master/utility.py

        with BufrFile(bufr_path, compressed=compressed) as bufr:
            for bufr_msg in bufr.get_messages():
                num_subsets = bufr_msg.get_value("numberOfSubsets")

                datetimes = bufr_msg.get_datetimes()

                raw_lats, raw_lons = bufr_msg.get_locations()
                raw_centre_freqs = bufr_msg.get_value("satelliteChannelCentreFrequency")
                raw_pressures = bufr_msg.get_value("pressure")

                raw_wind_speeds = bufr_msg.get_value("windSpeed")
                raw_wind_dirs = bufr_msg.get_value("windDirection")

                DELAY_REPLICATION_NUM = 6
                replication_factors = bufr_msg.get_value("delayedDescriptorReplicationFactor")

                # Variables are sometimes a single value per message
                # (all subset vals the same or one subset)
                # So duplicate them into an array
                if not isinstance(raw_centre_freqs, Iterable):
                    raw_centre_freqs = full(datetimes.shape, raw_centre_freqs)

                if not isinstance(raw_wind_speeds, Iterable):
                    raw_wind_speeds = full(datetimes.shape, raw_wind_speeds)

                if not isinstance(raw_wind_dirs, Iterable):
                    raw_wind_dirs = full(datetimes.shape, raw_wind_dirs)

                # Setup the arrays for this message
                lats, lons = [], []
                centre_freqs, pressures = [], []
                wind_speeds, wind_dirs = [], []
                qis = []

                # Setup the offsets and indices for looping though this message
                lat_lon_offset = 0
                pressure_offset = 0
                centre_freq_offset = 0
                wind_speed_dir_offset = 0

                lat_lon_index = 0
                pressure_index = 0
                centre_freq_index = 0
                wind_speed_dir_index = 0

                for i in range(num_subsets):
                    # Generate the indices
#                    print(f"{i+1} / {num_subsets}")
                    lat_lon_index += lat_lon_offset
                    pressure_index += pressure_offset
                    centre_freq_index += centre_freq_offset
                    wind_speed_dir_index += wind_speed_dir_offset

#                    print("\tLat/Lon:", lat_lon_index, lat_lon_offset)
#                    print("\tPressure:", pressure_index, pressure_offset)
#                    print("\tCentre Freq:", centre_freq_index, centre_freq_offset)
#                    print("\tWind Speed/Dir:", wind_speed_dir_index, wind_speed_dir_offset)
#                    print()

                    # Append to the arrays
                    lats.append(raw_lats[lat_lon_index])
                    lons.append(raw_lons[lat_lon_index])

                    pressures.append(raw_pressures[pressure_index])
                    centre_freqs.append(raw_centre_freqs[centre_freq_index])

                    wind_speeds.append(raw_wind_speeds[wind_speed_dir_index])
                    wind_dirs.append(raw_wind_dirs[wind_speed_dir_index])

                    # Generate the next set of indices
                    # See link to Vincent's code above for explanation
                    lat_lon_offset = 1 + replication_factors[(i * DELAY_REPLICATION_NUM) + 2]
                    pressure_offset = 1 + replication_factors[(i * DELAY_REPLICATION_NUM)] + \
                        replication_factors[(i * DELAY_REPLICATION_NUM) + 1] + 3 + 1
                    centre_freq_offset = 1 + replication_factors[(i * DELAY_REPLICATION_NUM) + 1]
                    wind_speed_dir_offset = 1

                    # Quality Indicators
                    # We want the percentConfidence when standardGeneratingApplication is equal to:
                    #  4 -> Common IWWG QI
                    #  5 -> QI without forecast
                    #  6 -> QI with forecast
                    # There are 4 generating app/percent confidence pairs per subset

                    # I'm GUESSing we're interested in QI without forecast
                    # TODO: Determine exactly which one we want.
                    TARGET_GENERATING_APP = 5

                    # Can't seem to get the lot of these fields as array like with the others
                    # Perhaps because some of them are MISSING?
                    # Getting them individually here instead. I.e. #1#blah for the first one.

                    qi = None
                    for j in range(4):
                        # Index in bufr file starts from one, 4 per subset
                        gen_app_index = 1 + i*4 + j

                        gen_app = bufr_msg.get_value(f"#{gen_app_index}#standardGeneratingApplication")

                        if gen_app is TARGET_GENERATING_APP:
                            qi = bufr_msg.get_value(f"#{gen_app_index}#percentConfidence")

                            break

                    # This will break when there's no matching generating application. Fix it when it happens.
                    if qi is None:
                        raise IOError("No matching QI element found. "
                                      "Probably need to do something about this case.")
                    
                    qis.append(qi)

                small_df = DataFrame(zip(datetimes, pressures, lons, lats,
                                         wind_speeds, wind_dirs, centre_freqs, qis),
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
    cycle = "20200306T0000Z"
    df_c3, df_nwcsaf = get_data_for_cycle(cycle)

    print("C3 Dataframe:")
    df_c3.info()
    print()
    print("NWCSAF Dataframe:")
    df_nwcsaf.info()
    print()

    # Obs Counts
    print("Number of obs in C3 dataframe: {:,d}".format(df_c3.size))
    print("Number of obs in NWCSAF dataframe: {:,d}".format(df_nwcsaf.size))
    print()

    # Filter the dataframes to look at a single time only
    count = 0
    target = 0
    filter_time = None
    for c3_time in df_c3["datetime"].unique()[::-1]:
        for nwcsaf_time in df_nwcsaf["datetime"].unique():
            if c3_time == nwcsaf_time:
                if count >= target:
                    filter_time = c3_time

                    break
                else:
                    count += 1

        if filter_time:
            break

    print("Filtering dataframe - datetime =", filter_time)
    df_c3 = df_c3[filter_time == df_c3['datetime']]
    df_nwcsaf = df_nwcsaf[filter_time == df_nwcsaf['datetime']]

    # Channel Frequencies
    for df, name in [(df_c3, "C3"), (df_nwcsaf, "NWCSAF")]:
        print(f"Available Frequencies/Wavelenths for {name}:")

        unique_fs = df["channel centre frequency"].unique()
        unique_fs.sort()
        for f in unique_fs:
            print("{:.2f} THz / {:.2f} um".format(f*1e-12, 1e6 * 3e8 / f))

        print()

    # Let's filter on a 2 similar freq channels
    target_f_c3 = 2.72727e13
    target_f_nwcsaf = 2.680e+13

    df_c3 = df_c3[
        (0.98*target_f_c3 < df_c3['channel centre frequency']) &
        (df_c3['channel centre frequency'] < 1.02*target_f_c3)
    ]

    df_nwcsaf = df_nwcsaf[
        (0.975*target_f_nwcsaf < df_nwcsaf['channel centre frequency']) &
        (df_nwcsaf['channel centre frequency'] < 1.025*target_f_nwcsaf)
    ]

    # Filter on pressure level range
    p_min, p_max = 20000, 30000

    def filter_df_for_pressure(df, pmin, pmax):
        return df[(pmin < df['pressure']) & (df['pressure'] < pmax)]

    # Build a list of counts at pressure levels
    pmins = []
    c3_pressure_counts = []
    nwcsaf_pressure_counts = []
    d_pressure = 10000
    for pmin in range(10000, 100000, d_pressure):
        pmins.append(pmin)

        c3_len = len(filter_df_for_pressure(df_c3, pmin, pmin+d_pressure))
        nwcsaf_len = len(filter_df_for_pressure(df_nwcsaf, pmin, pmin+d_pressure))

        c3_pressure_counts.append(c3_len)
        nwcsaf_pressure_counts.append(nwcsaf_len)

    # Now actually filter
    print(f"Filtering pressure from {p_min} to {p_max} hPa.")
    print()

    df_c3 = df_c3[
        (p_min < df_c3['pressure']) &
        (df_c3['pressure'] < p_max)
    ]

    df_nwcsaf = df_nwcsaf[
        (p_min < df_nwcsaf['pressure']) &
        (df_nwcsaf['pressure'] < p_max)
    ]

    print("Number of filtered obs in C3 dataframe: {:,d}".format(df_c3.size))
    print("Number of filtered obs in NWCSAF dataframe: {:,d}".format(df_nwcsaf.size))
    print()

    do_zoom = True
    if do_zoom:
        # Grab a subset for closer examination
        # Not mean or std at the moment since I've cherry picked an area
        nwcsaf_mean_lon = 123
        nwcsaf_mean_lat = -10
        nwcsaf_std_lon = 5
        nwcsaf_std_lat = 2.5
        # df_nwcsaf['longitude'].mean()
        # df_nwcsaf['latitude'].mean()
        # df_nwcsaf['longitude'].std()
        # df_nwcsaf['latitude'].std()

        # Filter on the zoomed subset
        f = 1
        df_nwcsaf_filtered = df_nwcsaf[
            ((nwcsaf_mean_lon - f*nwcsaf_std_lon) < df_nwcsaf.longitude) &
            (df_nwcsaf.longitude < (nwcsaf_mean_lon + f*nwcsaf_std_lon)) &
            ((nwcsaf_mean_lat - f*nwcsaf_std_lat) < df_nwcsaf.latitude) &
            (df_nwcsaf.latitude < (nwcsaf_mean_lat + f*nwcsaf_std_lat))
        ]

        df_c3_filtered = df_c3[
            ((nwcsaf_mean_lon - f*nwcsaf_std_lon) < df_c3.longitude) &
            (df_c3.longitude < (nwcsaf_mean_lon + f*nwcsaf_std_lon)) &
            ((nwcsaf_mean_lat - f*nwcsaf_std_lat) < df_c3.latitude) &
            (df_c3.latitude < (nwcsaf_mean_lat + f*nwcsaf_std_lat))
        ]

    ### Plotting
    figsize = 24, 6
    fig = plt.figure(cycle, figsize=figsize)

    ncols = 4
    nrows = 1
    subplot_index = 0

    subplot_spec = gridspec.GridSpec(nrows, ncols, wspace=0.2, hspace=0.4)

    plt.suptitle(cycle + " - {:.2f}THz Channel - {} to {} hPa".format(target_f_c3*1e-12, p_min, p_max))

    # Plot lat/lon for the full channel
    ax = plt.Subplot(fig, subplot_spec[subplot_index])
    subplot_index += 1

    ax.set_title("Lat/Lon for {:.2f} THz Channel".format(target_f_c3*1e-12))
    ax.plot(df_c3['longitude'], df_c3['latitude'], '+', color='r', label='C3')#, transform=crs.PlateCarree())
    ax.plot(df_nwcsaf['longitude'], df_nwcsaf['latitude'], 'x', color='b', label="NWCSAF")#, transform=crs.PlateCarree())

    if do_zoom:
        ax.plot(df_nwcsaf_filtered['longitude'], df_nwcsaf_filtered['latitude'], 'x', color='g', label="NWCSAF (zoomed)")

    ax.legend(loc='lower right')

    ax.set_xlabel("Longitude (degrees)")
    ax.set_ylabel("Latitude (degrees)")

    fig.add_subplot(ax)

    if do_zoom:
        # Plot lat/lon for the subset
        ax = plt.Subplot(fig, subplot_spec[subplot_index])
        subplot_index += 1

        ax.set_title("Zoomed in Lat/Lon for {:.2f} THz Channel".format(target_f_c3*1e-12))
        ax.plot(df_nwcsaf_filtered['longitude'], df_nwcsaf_filtered['latitude'], 'x', color='b', label="NWCSAF")
        ax.plot(df_c3_filtered['longitude'], df_c3_filtered['latitude'], '+', color='r', label='C3')

        ax.legend(loc='lower left')

        ax.set_xlabel("Longitude (degrees)")
        ax.set_ylabel("Latitude (degrees)")

        fig.add_subplot(ax)

    # Plot a histogram of wind speed and of wind dir for subsets
    sub_subplot_spec = gridspec.GridSpecFromSubplotSpec(2, 2,
        subplot_spec=subplot_spec[subplot_index], wspace=0.2, hspace=0.2)

    subplot_index += 1
    sub_subplot_index = 0

    # NWCSAF wind speed histogram
    ax = plt.Subplot(fig, sub_subplot_spec[sub_subplot_index])
    sub_subplot_index += 1

    ax.set_title("NWCSAF")

    if do_zoom:
#        ax.suptitle("Zoomed Region")
        wind_speed = df_nwcsaf_filtered["wind_speed"]
    else:
        wind_speed = df_nwcsaf["wind_speed"]
    ax.hist(wind_speed, 100, label="Wind Speed - NWCSAF", color='b')
    s = "Mean: {:.2f}\nStd: {:.2f}".format(wind_speed.mean(), wind_speed.std())
    ax.annotate(s, (0, 0), (0.55, 0.8), xycoords="axes fraction")

    ax.set_xlabel("Wind Speed (m/s)")
    ax.set_ylabel("Occurance")

    fig.add_subplot(ax)

    # C3's wind speed histogram
    ax = plt.Subplot(fig, sub_subplot_spec[sub_subplot_index])
    sub_subplot_index += 1
    ax.set_title("C3")

    if do_zoom:
        wind_speed = df_c3_filtered["wind_speed"]
    else:
        wind_speed = df_c3["wind_speed"]
    ax.hist(wind_speed, 100, label="Wind Speed - C3", color='r')

    s = "Mean: {:.2f}\nStd: {:.2f}".format(wind_speed.mean(), wind_speed.std())
    ax.annotate(s, (0, 0), (0.55, 0.8), xycoords="axes fraction")

    ax.set_xlabel("Wind Speed (m/s)")

    fig.add_subplot(ax)

    # NWCSAF wind dir histogram
    ax = plt.Subplot(fig, sub_subplot_spec[sub_subplot_index])
    sub_subplot_index += 1
    if do_zoom:
        wind_dir = df_nwcsaf_filtered["wind_direction"]
    else:
        wind_dir = df_nwcsaf["wind_direction"]
    ax.hist(wind_dir, 100, label="Wind Direction - NWCSAF", color='b')

    s = "Mean: {:.2f}\nStd: {:.2f}".format(wind_dir.mean(), wind_dir.std())
    ax.annotate(s, (0, 0), (0.55, 0.8), xycoords="axes fraction")

    ax.set_xlabel("Wind Direction (degrees)")
    ax.set_ylabel("Occurance")

    fig.add_subplot(ax)

    # C3's wind dir histogram
    ax = plt.Subplot(fig, sub_subplot_spec[sub_subplot_index])
    sub_subplot_index += 1
    if do_zoom:
        wind_dir = df_c3_filtered["wind_direction"]
    else:
        wind_dir = df_c3["wind_direction"]
    ax.hist(wind_dir, 100, label="Wind Direction - C3", color='r')

    s = "Mean: {:.2f}\nStd: {:.2f}".format(wind_dir.mean(), wind_dir.std())
    ax.annotate(s, (0, 0), (0.55, 0.8), xycoords="axes fraction")

    ax.set_xlabel("Wind Direction (degrees)")

    fig.add_subplot(ax)

    # Temporal profile
    if False:
        plt.figure()

        plt.title("Time vs. Latitude")
        plt.plot(df_c3['datetime'], df_c3['latitude'], '+', color='r', label='C3')
        plt.plot(df_nwcsaf['datetime'], df_nwcsaf['latitude'], 'x', color='b', label="NWCSAF")

        plt.xlabel("Time (datetime)")
        plt.ylabel("Latitude (degrees)")

        plt.legend(loc='lower left')

    # Show the pressure level counts
    ax = plt.Subplot(fig, subplot_spec[subplot_index])
    subplot_index += 1

    ax.set_title("Obs Counts at Pressure levels")

    ax.plot(pmins, c3_pressure_counts, color='r', label='C3')
    ax.plot(pmins, nwcsaf_pressure_counts, color='b', label="NWCSAF")

    ax.axhline(0, color='k', linestyle='--')

    ax.axvline(p_min, color='grey', linestyle=':')
    ax.axvline(p_max, color='grey', linestyle=':')

    ax.set_xlabel("Pressure (hPa)")
    ax.set_ylabel("Obs Count")

    ax.legend(loc='lower left')

    fig.add_subplot(ax)


    # Plot the QIs
    c3_lon = df_c3_filtered['longitude']
    c3_lat = df_c3_filtered['latitude']
    c3_qi = df_c3_filtered["quality indicator"]

    nwcsaf_lon = df_nwcsaf_filtered['longitude']
    nwcsaf_lat = df_nwcsaf_filtered['latitude']
    nwcsaf_qis = df_nwcsaf_filtered["quality indicator"]

    min_x = min(c3_lon.min(), nwcsaf_lon.min())
    max_x = max(c3_lon.max(), nwcsaf_lon.max())
    min_y = min(c3_lat.min(), nwcsaf_lat.min())
    max_y = max(c3_lat.max(), nwcsaf_lat.max())
    min_z = min(c3_qi.min(), nwcsaf_qis.min())
    max_z = max(c3_qi.max(), nwcsaf_qis.max())

    cmap = 'jet'

    plt.figure()

    plt.suptitle("Quality Indicators")

    c3_ax = plt.subplot(1, 2, 1)
    plt.title("C3 - probably wrong")

    plt.scatter(c3_lon, c3_lat,
                c=c3_qi, cmap=cmap,
                vmin=min_z, vmax=max_z)

    plt.xlim((min_x, max_x))
    plt.ylim((min_y, max_y))

    plt.xlabel("Longitude (degrees)")
    plt.ylabel("Latitude (degrees)")

    nwcsaf_ax = plt.subplot(1, 2, 2)
    plt.title("NWCSAF")

    plt.scatter(nwcsaf_lon, nwcsaf_lat,
                c=nwcsaf_qis, cmap=cmap,
                vmin=min_z, vmax=max_z)

    plt.xlim((min_x, max_x))
    plt.ylim((min_y, max_y))

    plt.xlabel("Longitude (degrees)")
    plt.ylabel("Latitude (degrees)")

    plt.colorbar(ax=(c3_ax, nwcsaf_ax))


    plt.show()


if __name__ == "__main__":
    main()
