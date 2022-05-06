
## IMPORTS
from glob import glob
from os import replace, remove
from os.path import join, isdir, basename, exists
from sys import path, argv
from datetime import datetime
from pandas import read_csv

# Import custom modules
path.insert(1, "/g/data/hd50/jt4085/BARRA2/util/bufr")
from eccodes_wrapper import BufrFile


## PARAMETERS
SONDE_TYPE_DIR = "/scratch/hd50/jt4085/sonde/sonde_types"
SONDE_BUFR_DIR = "/scratch/hd50/jt4085/sonde/data-bufr-bins"


## FUNCTIONS
def get_type_for_station(station_number, block_number,
    ship_or_mobile_station_number, dt, type_csv_file):
    csv = read_csv(type_csv_file, parse_dates=["Datetime"])

    # Handle the "MISSING" id numbers and get the matching indices from CSV
    if station_number == "MISSING":
        station_number_indices = csv["WMO Station Number"].isna() 
    else:
        station_number_indices = csv["WMO Station Number"] == station_number

    if block_number == "MISSING":
        block_number_indices = csv["WMO Block Number"].isna()
    else:
        block_number_indices = csv["WMO Block Number"] == block_number

    if ship_or_mobile_station_number == "MISSING":
        ship_number_indices = csv["Ship or Mobile Station Identifier"].isna()
    else:
        ship_number_indices = csv["Ship or Mobile Station Identifier"] == \
            ship_or_mobile_station_number

    filtered = csv[
        station_number_indices & block_number_indices & ship_number_indices
    ]

    # Filterout all rows after dt
    # Grab the most recent one for the most up to date type
    filtered = filtered[filtered["Datetime"] <= dt]
    if len(filtered) > 0:
        latest_row = filtered.iloc[-1]

        return latest_row["Radiosonde Type"]
    
    return None

def update_type_in_bufr_file(bufr_file, type_csv_file):
    temp_out_file = bufr_file + ".temp"

    # Remove temp_out_file if it exists so we don't append to
    # a previous attempt.
    if exists(temp_out_file):
        remove(temp_out_file)

    with BufrFile(bufr_file) as bufr:
        for msg in bufr.get_messages():
            sonde_type = msg.get_value("radiosondeType")

            if sonde_type != "MISSING":
                print("\t\t\t\tSonde Type already updated.")
            else:
                station_number = msg.get_value("stationNumber")
                block_number = msg.get_value("blockNumber")
                ship_or_mobile_station_number = \
                    msg.get_value("shipOrMobileLandStationIdentifier")

                year = msg.get_value("year")
                month = msg.get_value("month")
                day = msg.get_value("day")
                hour = msg.get_value("hour")
                minute = msg.get_value("minute")
                dt = datetime(year=year, month=month, day=day,
                              hour=hour, minute=minute)

                new_type = get_type_for_station(station_number, block_number,
                    ship_or_mobile_station_number, dt, type_csv_file)

                print("\t\t\t\tStation Number:", station_number)
                print("\t\t\t\tBlock Number:", block_number)
                print("\t\t\t\tShip or Mobile Station Number:",
                    ship_or_mobile_station_number)
                print("\t\t\t\tDatetime:", dt)

                print("\t\t\t\tOld Type:", sonde_type)
                print("\t\t\t\tNew Type:", new_type)
                print()

                if new_type is None or new_type == "MISSING":
                    # 255 is MISSING
                    new_type = 255

                msg.set_value("radiosondeType", int(new_type))

            with open(temp_out_file, 'ab') as temp_output_file:
                msg.write_to_file(temp_output_file)

    # Copy the temp file to the actual file.
    if temp_out_file:
        replace(temp_out_file, bufr_file)


## SCRIPT
def main():
    for y_dir in sorted(glob(join(SONDE_BUFR_DIR, "*"))):
        if not isdir(y_dir):
            continue

        y = basename(y_dir)
        print(y)


        if int(y) < 2007:
            print("\tBefore 2007, skipping")
            continue

        if y != argv[1]:
            print("\tYear not equal to", argv[1])
            continue

        for m_dir in sorted(glob(join(y_dir, "*"))):
            if not isdir(m_dir):
                continue

            m = basename(m_dir)
            print("\t" + m)

            sonde_type_file = join(SONDE_TYPE_DIR,
                "{year}-{month}_sonde_types.csv".format(year=y, month=m))

            if not exists(sonde_type_file):
                print("\t\tSonde type files doesn't exist!", sonde_type_file)
                exit()

            for dt_dir in sorted(glob(join(m_dir, "*"))):
                if ".broken" in dt_dir:
                    # There's a bad file I can't delete
                    # Skip this dir
                    continue

                dt = basename(dt_dir)
                print("\t\t" + dt)

                if dt < "20201202":
                    print("\t\t\tEarlier than 2007-11-20, skipping.")
                    continue

                bufr_files = sorted(glob(join(dt_dir, "*.bufr")))

                for bufr_filepath in bufr_files:
                    filename = basename(bufr_filepath)
                    print("\t\t\t" + filename)

                    update_type_in_bufr_file(bufr_filepath, sonde_type_file)


if __name__ == "__main__":
    main()

    print("Script finished.")

