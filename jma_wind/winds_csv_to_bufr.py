# This script converts satellite wind observations given as a CSV into the
# BUFR format.
#
# The input CSVs are expected to have the following columns:
# lon(deg.),lat(deg.),height(hPa),time(mjd),QI using NWP,QI not using NWP,u (m/s),v (m/s),satzenithangle(deg.) 
#
# Intended for use with the BARRA2 project.
# Author: Joshua Torrance

# IMPORTS
from argparse import ArgumentParser, ArgumentTypeError
from math import floor, sqrt
from numpy import sqrt, arctan2, degrees
from scipy.constants import c
from datetime import datetime, timezone
import eccodes as ecc

from jma_interface import get_wind_data

# PARAMETERS
# Commandline arg datetime format
COMMANDLINE_DT_FORMAT = "%Y%m%dT%H%M"

# We can build from a blank sample template
SAMPLE_TEMPLATE = "BUFR3_local_satellite"

# Length to separate the data into
BUFR_MESSAGE_LEN = 550

# Bufr Sequence
UNEXPANDED_DESCRIPTORS = [
    310014, 222000, 236000, 101103,  31031,
      1031,   1032, 101004,  33007, 222000,
    237000,   1031,   1032, 101004,  33035,
    222000, 237000,   1031,   1032, 101004,
     33036, 222000, 237000,   1031,   1032,
    101004,  33007, 222000, 237000,   1031,
      1032, 101004,  33035, 222000, 237000,
      1031,   1032, 101004,  33036, 222000,
    237000,   1031,   1032, 101004,  33007,
    222000, 237000,   1031,   1032, 101004,
     33035, 222000, 237000,   1031,   1032,
    101004, 33036
]

DATA_PRESENT_BITMAP = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 0, 0, 0, 1, 1,
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1
]

# Satellites
#  https://confluence.ecmwf.int/display/ECC/WMO%3D2+code-flag+table#WMO=2codeflagtable-CF_001007
#  centre is the central wavelength in microns, bandwidth is also in microns
SATELLITE_NAMES = ["GMS-5", "MTSAT-1R", "MTSAT-2", "GOES-9"]
CHANNEL_NAMES = ["VIS", "IR1", "IR2", "IR3", "IR4"]
# OPS only uses VIS, IR1 and IR3
CHANNEL_NAMES = ["VIS", "IR1", "IR3"]
SATELLITES = {"GMS-5": {
                "id": 152,
                "VIS": {"centre": 0.72, "bandwidth": 0.35},
                "IR1": {"centre": 11.0, "bandwidth": 0.50},
                "IR2": {"centre": 12.0, "bandwidth": 1.0},
                "IR3": {"centre": 6.75, "bandwidth": 0.50}
                },
              "MTSAT-1R": {
                "id": 171,
                "VIS": {"centre": 0.725, "bandwidth": 0.35},
                "IR1": {"centre":  10.8, "bandwidth": 1.0},
                "IR2": {"centre":  12.0, "bandwidth": 1.0},
                "IR3": {"centre":  6.75, "bandwidth": 0.50},
                "IR4": {"centre":  3.75, "bandwidth": 0.50}
                },
              "MTSAT-2": {
                "id": 172,
                "VIS": {"centre": 0.675, "bandwidth": 0.25},
                "IR1": {"centre":  10.8, "bandwidth": 1.0},
                "IR2": {"centre":  12.0, "bandwidth": 1.0},
                "IR3": {"centre":  6.75, "bandwidth": 0.50},
                "IR4": {"centre":  3.75, "bandwidth": 0.50}
                },
              "GOES-9": {
                "id": 253,
                "VIS": {"centre":  0.65, "bandwidth": 0.20},
                "IR1": {"centre": 10.70, "bandwidth": 1.0},
                "IR2": {"centre": 11.95, "bandwidth": 0.9},
                "IR3": {"centre":  6.57, "bandwidth": 0.50},
                "IR4": {"centre":  3.90, "bandwidth": 0.20}
                }
              }


# METHODS
def data_to_bufr(data, output_file,
                 satellite_name, channel_name):
    for key in data:
        # Potentially multiple datasets per data
        # They'll be written as serial messages
        # a - full dish, f - Northern hemisphere, s - Southern hemisphere
        full_dataframe = data[key]

        for i in range(0, len(full_dataframe), BUFR_MESSAGE_LEN):
            # Grab the next chunk of the data
            dataframe = full_dataframe[i:i + BUFR_MESSAGE_LEN]
            d_len = len(dataframe)

            # Create the bufr message from the sample.
            output_bufr = ecc.codes_bufr_new_from_samples(SAMPLE_TEMPLATE)

            # Set the data present bitmap for the quality info
            ecc.codes_set_long_array(output_bufr, 'inputDataPresentIndicator',
                                     DATA_PRESENT_BITMAP)

            # Set header values
            # These are set to be the same as in the template
            ecc.codes_set(output_bufr, 'edition', 3)
            ecc.codes_set(output_bufr, 'masterTableNumber', 0)
            ecc.codes_set(output_bufr, 'bufrHeaderCentre', 34)
            ecc.codes_set(output_bufr, 'bufrHeaderSubCentre', 0)
            ecc.codes_set(output_bufr, 'dataCategory', 5)
            ecc.codes_set(output_bufr, 'dataSubCategory', 87)
            ecc.codes_set(output_bufr, 'masterTablesVersionNumber', 8)
            ecc.codes_set(output_bufr, 'localTablesVersionNumber', 0)

            # Data set details
            ecc.codes_set(output_bufr, 'numberOfSubsets', d_len)
            ecc.codes_set(output_bufr, 'localNumberOfObservations', d_len)
            ecc.codes_set(output_bufr, 'observedData', 1)
            ecc.codes_set(output_bufr, 'compressedData', 1)

            # Set time values
            # For now set them to the fist time in the data set.
            first_dt = dataframe['time(mjd)'].iloc[0]
            ecc.codes_set(output_bufr, 'typicalCentury', floor(first_dt.year / 100))
            ecc.codes_set(output_bufr, 'typicalYearOfCentury', first_dt.year % 100)
            ecc.codes_set(output_bufr, 'typicalMonth', first_dt.month)
            ecc.codes_set(output_bufr, 'typicalDay', first_dt.day)
            ecc.codes_set(output_bufr, 'typicalHour', first_dt.hour)
            ecc.codes_set(output_bufr, 'typicalMinute', first_dt.minute)

            # BUFR Sequence
            ecc.codes_set_long_array(output_bufr, 'unexpandedDescriptors',
                                     UNEXPANDED_DESCRIPTORS)

            # Satellite details
            sat_id = SATELLITES[satellite_name]["id"]
            # Central wavelength (wl) and frequency (fq)
            sat_centre_wl = SATELLITES[satellite_name][channel_name]["centre"] * 1e-6
            sat_centre_fq = c / sat_centre_wl

            # Bandwidth in wavelength (wl) and frequency (fq)
            sat_bandwidth_wl = SATELLITES[satellite_name][channel_name]["bandwidth"] * 1e-6
            sat_bandwidth_fq = c / (sat_centre_wl - 0.5 * sat_bandwidth_wl) - \
                               c / (sat_centre_wl + 0.5 * sat_bandwidth_wl)

            ecc.codes_set(output_bufr, 'satelliteID',
                          sat_id)
            ecc.codes_set_array(output_bufr, 'satelliteIdentifier',
                                [sat_id]*d_len)
            ecc.codes_set_array(output_bufr, 'satelliteChannelCentreFrequency',
                                [sat_centre_fq]*d_len)
            ecc.codes_set_array(output_bufr, 'satelliteChannelBandWidth',
                                [sat_bandwidth_fq]*d_len)

            # Computation method seems to depend on channel
            # Options here - https://confluence.ecmwf.int/display/ECC/WMO%3D14+code-flag+table#WMO=14codeflagtable-CF_002023
            # Apparently this should be 3?
            computationMethod = 3
            ecc.codes_set_array(output_bufr,
                                'satelliteDerivedWindComputationMethod',
                                [computationMethod]*d_len)

            # Originating Centre - JMA - 34
            # Turns out this needs to be set for every subset.
            ecc.codes_set_array(output_bufr, '#1#centre', [34]*d_len)

            # Set the data arrays
            set_arrays_for_dataframe(dataframe, output_bufr)

            # Finish the file
            ecc.codes_set(output_bufr, 'pack', 1)

            ecc.codes_write(output_bufr, output_file)

            ecc.codes_release(output_bufr)


def set_arrays_for_dataframe(dataframe, output_bufr):
    # Longitude & Latitude
    ecc.codes_set_array(output_bufr, 'longitude',
                        dataframe['lon(deg.)'].to_numpy())
    ecc.codes_set_array(output_bufr, 'latitude',
                        dataframe['lat(deg.)'].to_numpy())

    # Height/Pressure
    # 1 Pa = 100 hPa
    ecc.codes_set_array(output_bufr, '#1#pressure',
                        (dataframe['height(hPa)'] * 100).to_numpy())

    # Time
    ecc.codes_set_array(output_bufr, '#1#year',
                        [int(i) for i in dataframe['time(mjd)'].dt.year])
    ecc.codes_set_array(output_bufr, '#1#month',
                        [int(i) for i in dataframe['time(mjd)'].dt.month])
    ecc.codes_set_array(output_bufr, '#1#day',
                        [int(i) for i in dataframe['time(mjd)'].dt.day])
    ecc.codes_set_array(output_bufr, '#1#hour',
                        [int(i) for i in dataframe['time(mjd)'].dt.hour])
    ecc.codes_set_array(output_bufr, '#1#minute',
                        [int(i) for i in dataframe['time(mjd)'].dt.minute])
    ecc.codes_set_array(output_bufr, '#1#second',
                        [int(i) for i in dataframe['time(mjd)'].dt.second])

    # Wind speed and velocity
    u = dataframe['u (m/s)']
    v = dataframe['v (m/s)']

    # Wind direction is a bit odd in the meteorological context.
    # More info: http://colaweb.gmu.edu/dev/clim301/lectures/wind/wind-uv
    wind_speed = sqrt(u ** 2 + v ** 2)
    wind_direction = (270 - degrees(arctan2(v, u))) % 360

    ecc.codes_set_array(output_bufr, '#1#windSpeed',
                        wind_speed.to_numpy())
    ecc.codes_set_array(output_bufr, '#1#windDirection',
                        wind_direction.to_numpy())

    # Quality Index
    # QI*100 = percent confidence
    # TODO: Does the QI apply directly like this?
    #           Do the u & v errors need combined for windSpeed/Dir?
    # TODO: What does the QI apply to?
    #           Measurement (u & v)? Coordinates (lat, lon, pres)?
    ecc.codes_set_array(output_bufr, '#1#windSpeed->percentConfidence',
                        (100 * dataframe['QI using NWP']).to_numpy())
    ecc.codes_set_array(output_bufr, '#1#windDirection->percentConfidence',
                        (100 * dataframe['QI using NWP']).to_numpy())

    # Setting more quality info to try to get things working
    # TODO: Figure this out and do it properly
    ecc.codes_set_array(output_bufr,
        '#1#pressure->percentConfidence',
        (100 * dataframe['QI using NWP']).to_numpy())
    ecc.codes_set_array(output_bufr,
        '#1#pressure->percentConfidence->percentConfidence',
        (100 * dataframe['QI not using NWP']).to_numpy())
    ecc.codes_set_array(output_bufr,
        '#1#pressure->percentConfidence->percentConfidence->percentConfidence',
        (100 * dataframe['QI not using NWP']).to_numpy())

    ecc.codes_set_array(output_bufr,
        '#1#pressure->nominalConfidenceThreshold',
        [0]*len(dataframe))
    ecc.codes_set_array(output_bufr,
        '#1#pressure->nominalConfidenceThreshold->nominalConfidenceThreshold',
        [0]*len(dataframe))
    ecc.codes_set_array(output_bufr,
        '#1#pressure->nominalConfidenceThreshold->nominalConfidenceThreshold->nominalConfidenceThreshold',
        [0]*len(dataframe))

    ecc.codes_set_array(output_bufr,
        '#1#windSpeed->percentConfidence->percentConfidence',
        (100 * dataframe['QI not using NWP']).to_numpy())
    ecc.codes_set_array(output_bufr,
        '#1#windSpeed->percentConfidence->percentConfidence->percentConfidence',
        (100 * dataframe['QI not using NWP']).to_numpy())

    ecc.codes_set_array(output_bufr,
        '#1#windSpeed->nominalConfidenceThreshold',
        [0]*len(dataframe))
    ecc.codes_set_array(output_bufr,
        '#1#windSpeed->nominalConfidenceThreshold->nominalConfidenceThreshold',
        [0]*len(dataframe))
    ecc.codes_set_array(output_bufr,
        '#1#windSpeed->nominalConfidenceThreshold->nominalConfidenceThreshold->nominalConfidenceThreshold',
        [0]*len(dataframe))

    ecc.codes_set_array(output_bufr,
        '#1#windDirection->percentConfidence->percentConfidence',
        (100 * dataframe['QI not using NWP']).to_numpy())
    ecc.codes_set_array(output_bufr,
        '#1#windDirection->percentConfidence->percentConfidence->percentConfidence',
        (100 * dataframe['QI not using NWP']).to_numpy())
    
    ecc.codes_set_array(output_bufr,
        '#1#windDirection->nominalConfidenceThreshold',
        [0]*len(dataframe))
    ecc.codes_set_array(output_bufr,
        '#1#windDirection->nominalConfidenceThreshold->nominalConfidenceThreshold',
        [0]*len(dataframe))
    ecc.codes_set_array(output_bufr,
        '#1#windDirection->nominalConfidenceThreshold->nominalConfidenceThreshold->nominalConfidenceThreshold',
        [0]*len(dataframe))

    # Satellite Zenith Angle
    ecc.codes_set_array(output_bufr, 'satelliteZenithAngle',
                        dataframe['satzenithangle(deg.)'].to_numpy())


# SCRIPT
def parse_args():
    def valid_date(s):
        try:
            # Parse the input string
            # These timezones are timezone naive to match
            #   those in jma_interface.
            # return datetime.strptime(s + "+0000", COMMANDLINE_DT_FORMAT + "%z")
            return datetime.strptime(s, COMMANDLINE_DT_FORMAT)
        except ValueError:
            msg = "not a valid date: {0!r}".format(s)
            raise ArgumentTypeError(msg)

    parser = ArgumentParser(prog="JMA Winds - CSV to BUFR",
                            description="This script converts JMA Winds data"
                                        " from CSVs to BUFRs.\n"
                                        "Author: Joshua Torrance")

    parser.add_argument("-s", "--start",
                        nargs="?", required=True, type=valid_date,
                        help="Start UTC datetime to grab data for. Will be "
                             "aligned to the bin edge before it. "
                             "Use the format " + COMMANDLINE_DT_FORMAT)
    parser.add_argument("-e", "--end",
                        nargs="?", required=True, type=valid_date,
                        help="End UTC datetime to grab data for. Will be "
                             "aligned to the bin edge after it. "
                             "Use the format " + COMMANDLINE_DT_FORMAT)

    parser.add_argument("-c", "--channel",
                        nargs="?", required=False, default="all",
                        choices=CHANNEL_NAMES + ["all"],
                        help="Name of the channel to filter on or \"all\".")

    parser.add_argument("--satellite",
                        nargs="?", required=False, default="all",
                        choices=SATELLITE_NAMES + ["all"],
                        help="Name of the satellite to filter on or \"all\".")

    parser.add_argument("-o", "--output",
                        nargs="?", required=True,
                        help="Output file path.")

    return parser.parse_args()


def main():
    args = parse_args()

    start_dt = args.start
    end_dt = args.end

    satellite_filter = args.satellite
    channel_filter = args.channel

    output_filepath = args.output

    if satellite_filter == "all":
        satellite_list = SATELLITE_NAMES
    else:
        satellite_list = [satellite_filter]

    if channel_filter == "all":
        channel_list = CHANNEL_NAMES
    else:
        channel_list = [channel_filter]

    with open(output_filepath, 'wb') as f:
        for sat in satellite_list:
            for chan in channel_list:
                data = get_wind_data(sat, chan, start_dt, end_dt)

                if len(data) > 0:
                    data_to_bufr(data, f, sat, chan)


if __name__ == "__main__":
    main()
