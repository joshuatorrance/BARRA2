
## IMPORTS
from pandas import read_csv
from matplotlib import pyplot as plt
from glob import glob
from os.path import basename
from sys import path
from numpy import concatenate
from datetime import datetime
from random import shuffle

from sonde_bufr_check import get_station_location

# Import custom modules
path.insert(1, "/g/data/hd50/jt4085/BARRA2/util/bufr")
from eccodes_wrapper import BufrFile

## PARAMETERS
# Input files
DT = datetime(year=2019, month=1, day=2, hour=0)
DT = datetime(year=2010, month=6, day=2, hour=12)
DT = datetime(year=2007, month=10, day=22, hour=12)
DT = datetime(year=2019, month=4, day=22, hour=12)
PRODUCTION_DUMP = "/g/data/hd50/jt4085/BARRA2/sonde/data/{year}{month:02d}{day:02d}T{hour:02d}00Z_dump.csv" \
    .format(year=DT.year, month=DT.month, day=DT.day, hour=DT.hour)
CONVERTED_DIR = "/scratch/hd50/jt4085/sonde/data-bufr-bins/{year}/{month:02d}/{year}{month:02d}{day:02d}T{hour:02d}00Z/*-{year}{month:02d}{day:02d}{hour:02d}00.bufr" \
    .format(year=DT.year, month=DT.month, day=DT.day, hour=DT.hour)

# Barra region
BARRA_LEFT = 90.00
BARRA_RIGHT = 210.0 - 360
BARRA_BOTTOM = -60.00
BARRA_TOP = 15.00
barra_filter = (BARRA_LEFT, BARRA_RIGHT, BARRA_BOTTOM, BARRA_TOP)


# Script
def main():
    # Find a bufr in the barra region
    converted_bufrs = glob(CONVERTED_DIR)
    shuffle(converted_bufrs)
    for f in converted_bufrs:
        print(basename(f))

        station_name = basename(f)[:11]

        station_lat, station_lon = \
            get_station_location(station_name)

        print("\t", station_lat, station_lon)

        if (BARRA_BOTTOM < station_lat < BARRA_TOP) and \
            ((BARRA_LEFT < BARRA_RIGHT) and \
                 (BARRA_LEFT < station_lon < BARRA_RIGHT) or \
             (BARRA_LEFT > BARRA_RIGHT) and \
                 (BARRA_LEFT < station_lon or station_lon < BARRA_RIGHT)):
            print("\t\tIn region")

            station_filepath = f
            break

    # Get the data from the bufr
    station_wind_speed = []
    station_wind_dir = []
    station_air_temp = []
    station_pressure = []
    with BufrFile(station_filepath) as bufr:
        for msg in bufr.get_messages():
            station_wind_speed = concatenate((station_wind_speed,
                                              msg.get_value("windSpeed")))

            station_wind_dir = concatenate((station_wind_dir,
                                            msg.get_value("windDirection")))

            replication_factor = msg.get_value("extendedDelayedDescriptorReplicationFactor")

            pressure = []
            air_temp = []
            for i in range(replication_factor):
                val = msg.get_value("#{}#pressure".format(i+1))
                pressure.append(val if val!="MISSING" else float("NaN"))

                val = msg.get_value("#{}#airTemperature".format(i+1))
                air_temp.append(val if val!="MISSING" else float("NaN"))

            station_pressure = concatenate((station_pressure,
                                            pressure))
            station_air_temp = concatenate((station_air_temp,
                                            air_temp))

    # Replace missing values with NaN
    station_wind_speed = [ws if ws!=-1e100 else float("NaN")
                          for ws in station_wind_speed]
    station_wind_dir = [wd if wd!=2147483647 else float("NaN")
                        for wd in station_wind_dir]


    # Load the CSV
    prod_df = read_csv(PRODUCTION_DUMP)

    # We don't seem to need Replication Factor after all
    if "Replication Factor" in prod_df.columns:
        prod_df = prod_df.drop("Replication Factor", axis=1)

    cols_to_explode = ["Air Temperature", "Wind Direction", "Wind Speed"]

    # Split the strings into lists
    for col in cols_to_explode:
        prod_df[col] = prod_df[col].str.split()

    # Explode the columns together
    prod_df = prod_df.explode(cols_to_explode).reset_index()

    # Replace MISSING values with NaN
    missing_vals = ["-1e+100", "2147483647"]
    prod_df = prod_df.replace(to_replace=missing_vals, value='NaN')

    # Convert elements to numbers
    prod_df = prod_df.applymap(float)


    # Filter df to barra region
    if BARRA_LEFT < BARRA_RIGHT:
        barra_df = prod_df[(BARRA_LEFT < prod_df['Longitude']) &
                           (prod_df['Longitude'] < BARRA_RIGHT)]
    else:
        barra_df = prod_df[(BARRA_LEFT < prod_df['Longitude']) |
                           (prod_df['Longitude'] < BARRA_RIGHT)]

    barra_df = barra_df[(BARRA_BOTTOM < barra_df['Latitude']) &
                        (barra_df['Latitude'] < BARRA_TOP)]

    # Filter df to the station
    d = 1 # Lat/Lon margin around station
    left = station_lon - d
    right = station_lon + d
    bottom = station_lat - d
    top = station_lat + d

    station_df = barra_df[(left < barra_df['Longitude']) &
                          (barra_df['Longitude'] < right) &
                          (bottom < barra_df['Latitude']) &
                          (barra_df['Latitude'] < top)]
    station_df = station_df.reset_index()

    # Print latitude and longitude
    plt.figure("Map")
    plt.plot(prod_df['Longitude'], prod_df['Latitude'], 'x')
    plt.plot(barra_df['Longitude'], barra_df['Latitude'], '+')

    plt.axhline(BARRA_BOTTOM, linestyle='-', color='r')
    plt.axhline(BARRA_TOP, linestyle='-', color='r')
    plt.axvline(BARRA_LEFT, linestyle='-', color='r')
    plt.axvline(BARRA_RIGHT, linestyle='-', color='r')

    plt.plot(station_lon, station_lat, 'x')
    plt.plot(station_df['Longitude'], station_df['Latitude'], '+')

    plt.xlabel("Longitude (degrees)")
    plt.ylabel("Latitude (degrees)")

    # Print station measurements
    plt.figure("Station measurements")

    for i in range(len(station_df)):
        print(i)
        print("\t", station_air_temp[i],
            type(station_air_temp[i]))
        print("\t", station_df['Air Temperature'][i],
            type(station_df['Air Temperature'][i]))

        print()

        print("\t", station_wind_dir[i],
            type(station_wind_dir[i]))
        print("\t", station_df['Wind Direction'][i],
            type(station_df['Wind Direction'][i]))

        print()

        print("\t", station_wind_speed[i],
            type(station_wind_speed[i]))
        print("\t", station_df['Wind Speed'][i],
            type(station_df['Wind Speed'][i]))

        print()

    plt.subplot(311)
    plt.plot(station_air_temp, 'x', label="Converted")
    plt.plot(station_df['Air Temperature'], '+', label="Production")
    plt.ylabel("Air Temperature (K)")

    plt.subplot(312)
    plt.plot(station_wind_dir, 'x', label="Converted")
    plt.plot(station_df['Wind Direction'], '+', label="Production")
    plt.ylabel("Wind Direction (degrees)")

    plt.subplot(313)
    plt.plot(station_wind_dir, 'x', label="Converted")
    plt.plot(station_df['Wind Direction'], '+', label="Production")
    plt.ylabel("Wind Speed (m/s)")

    plt.xlabel("Measurement Index")

    plt.legend(loc="lower left")


    plt.show()



if __name__ == "__main__":
    main()

