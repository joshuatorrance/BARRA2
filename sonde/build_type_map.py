

## IMPORTS
from glob import glob
from os import readlink
from os.path import basename, join, exists, islink, isabs, dirname, isdir
from sys import path
from datetime import datetime
from pandas import DataFrame, concat, read_csv

# Import custom modules
path.insert(1, "/g/data/hd50/jt4085/BARRA2/util/bufr")
from eccodes_wrapper import BufrFile


## PARAMETERS
#OLD_BUFR_DIR = "/scratch/hd50/jt4085/sonde/old_production_bufrs"
OLD_BUFR_DIR = "/g/data/hd50/barra2/data/obs/production"
OUTPUT_DIR = "/scratch/hd50/jt4085/sonde/sonde_types"


## Functions
def get_values_from_bufr(filepath):
    data = []
    with BufrFile(filepath) as bufr:
        print("\t\t\t\tMessage Count:",
              bufr.get_number_messages())

        for msg in bufr.get_messages():
            lat, lon = msg.get_locations()

            station_number = None
            block_number = None
            try:
                station_number = msg.get_value("stationNumber")
                block_number = msg.get_value("blockNumber")
            except ValueError:
                # Some sequences don't have station of block number.
                pass

            ship_or_mobile_station_number = None
            try:
                ship_or_mobile_station_number = \
                    msg.get_value("shipOrMobileLandStationIdentifier")
            except ValueError:
                # Some sequences don't have a mobile station number.
                pass
 
            try:
                year = msg.get_value("year")
            except ValueError:
                # If I can't get the year skip this message.
                print("\t\t\t\tUnable to get year from BUFR. Skipping message.")
                continue
            month = msg.get_value("month")
            day = msg.get_value("day")
            hour = msg.get_value("hour")
            minute = msg.get_value("minute")
            dt = datetime(year=year, month=month, day=day,
                          hour=hour, minute=minute)

            sonde_type = msg.get_value("radiosondeType")

            if False:
                print("\t\t\t\tLat, Lon:", lat, lon)
                print("\t\t\t\tWMO Station number:", station_number)
                print("\t\t\t\tWMO Block Number:", block_number)
                print("\t\t\t\tShip or Mobile Station ID:",
                      ship_or_mobile_station_number)
                print("\t\t\t\tDatetime:", dt)
                print("\t\t\t\tRadiosonde Type:", sonde_type)
                print()

            data.append({
                "Latitude": lat,
                "Longitude": lon,
                "WMO Station Number": station_number,
                "WMO Block Number": block_number,
                "Ship or Mobile Station Identifier":
                    ship_or_mobile_station_number,
                "Datetime": dt,
                "Radiosonde Type": sonde_type
            })

    # Build a dataframe, drop duplicates radiosonde types for a given station
    # Dedup will be performed later too
    df = DataFrame(data=data) \
        .sort_values(by="Datetime") \
        .drop_duplicates(
            subset=["WMO Station Number", "WMO Block Number",
                    "Ship or Mobile Station Identifier", "Radiosonde Type"],
            keep='first')

    # Dedup df on 
    return df


def process_month(input_dir, output_path):
    # Does a data frame for this month already exist?
    if exists(output_path):
        print("\t\tType data for this month already exists, skipping.")
        return

    df = DataFrame([], columns=["Latitude", "Longitude",
                                "WMO Station Number",
                                "WMO Block Number",
                                "Ship or Mobile Station Identifier",
                                "Datetime", "Radiosonde Type"])

    for dt_dir in sorted(glob(join(input_dir, "*"))):
        dt = basename(dt_dir)
        print("\t\t" + dt)

        bufr_files = sorted(glob(join(dt_dir, "bufr", "sonde", "TEMP_*.bufr")))

        for bufr_filepath in bufr_files:
            filename = basename(bufr_filepath)
            print("\t\t\t" + filename)

            if islink(bufr_filepath):
                # Update the filepath with the link's target
                target = readlink(bufr_filepath)

                if not isabs(target):
                    target = join(dirname(bufr_filepath), target)

                bufr_filepath = target

            new_df = get_values_from_bufr(bufr_filepath)

            # Concatenate dataframe
            #  drop duplicate radiosonde types for a given station
            df = concat((df, new_df)) \
                .sort_values(by="Datetime") \
                .drop_duplicates(
                    subset=["WMO Station Number", "WMO Block Number",
                            "Ship or Mobile Station Identifier",
                            "Radiosonde Type"],
                    keep='first')

    # Output dataframe to file.
    df.to_csv(output_path, index=False)


## SCRIPT
def main():
    for y_dir in sorted(glob(join(OLD_BUFR_DIR, "*"))):
        if not isdir(y_dir):
            continue

        y = basename(y_dir)
        print(y)


        if int(y) < 2007:
            print("\tBefore 2007, skipping")
            continue

        for m_dir in sorted(glob(join(y_dir, "*"))):
            if not isdir(m_dir):
                continue

            m = basename(m_dir)
            print("\t" + m)

            if False and m!="01":
                print("\t\tNot 01, skipping")
                continue

            sonde_type_csv_path = join(OUTPUT_DIR,
                "{year}-{month}_sonde_types.csv".format(year=y, month=m))

            process_month(m_dir, sonde_type_csv_path)

if __name__ == "__main__":
    main()

