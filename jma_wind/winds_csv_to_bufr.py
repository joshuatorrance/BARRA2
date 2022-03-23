# This script converts satellite wind observations given as a CSV into the
# BUFR format.
#
# The input CSVs are expected to have the following columns:
# lon(deg.),lat(deg.),height(hPa),time(mjd),QI using NWP,QI not using NWP,u (m/s),v (m/s),satzenithangle(deg.) 
#
# A template BUFR file is required for the initial setup of the file.
#
# Intended for use with the BARRA2 project.
# Author: Joshua Torrance

# IMPORTS
from math import floor
from scipy.constants import c
from datetime import datetime, timezone
import eccodes as ecc

from jma_interface import get_wind_data

# PARAMETERS
# Template to build the bufrs from.
TEMPLATE_BUFR = "/g/data/hd50/jt4085/BARRA2/jma_wind/data/template.bufr"

# Sattelite IDs from:
#  https://confluence.ecmwf.int/display/ECC/WMO%3D2+code-flag+table#WMO=2codeflagtable-CF_001007
SATELLITE_IDS = {"GMS-5": 152,
                 "MTSAT-1R": 171,
                 "MTSAT-2": 172,
                 "GOES-9": 253}

# Measurement bands
# The central wavelenth of the measurement bands in Hz
MEASUREMENT_BANDS_CENTRAL_WAVELENGTH_HZ = \
    {"VIS": c/0.675e-6,
     "IR1": c/10.8e-6,
     "IR2": c/12.0e-6,
     "IR2": c/6.75e-6,
     "IR2": c/3.75e-6}

# METHODS
def data_to_bufr(data, output_filepath,
                 satellite_name, channel_name,
                 template_filepath=TEMPLATE_BUFR):
    for key in data:
        # Load the template BUFR
        with open(template_filepath, 'r') as f_template:
            output_bufr = ecc.codes_bufr_new_from_file(f_template)

        ecc.codes_set(output_bufr, 'unpack', 1)

        # Potentially multiple datasets per data
        # They'll be written as serial messages
        # a - full dish, f - Northern hemisphere, s - Southern hemisphere
        dataframe = data[key]

        # Set header values
        # These are set to be the same as in the template
        ecc.codes_set(output_bufr, 'edition', 3)
        ecc.codes_set(output_bufr, 'masterTableNumber', 0)
        ecc.codes_set(output_bufr, 'bufrHeaderCentre', 1)
        ecc.codes_set(output_bufr, 'dataCategory', 5)
        ecc.codes_set(output_bufr, 'dataSubCategory', 87)
        ecc.codes_set(output_bufr, 'masterTablesVersionNumber', 13)
        ecc.codes_set(output_bufr, 'localTablesVersionNumber', 1)

        # Satellite details
        ecc.codes_set(output_bufr, 'satelliteIdentifier',
                      SATELLITE_IDS[satellite_name])
        ecc.codes_set(output_bufr, 'satelliteChannelCentreFrequency',
                      MEASUREMENT_BANDS_CENTRAL_WAVELENGTH_HZ[channel_name])
        # TODO: See if we also know satelliteChannelBandWidth

        ecc.codes_set(output_bufr, 'compressedData', 1)
        ecc.codes_set(output_bufr, 'numberOfSubsets', 1)

        # Set time values
        # For now set them to the fist time in the data set.
        first_dt = dataframe['time(mjd)'][0]
        ecc.codes_set(output_bufr, 'typicalCentury', floor(first_dt.year/100))
        ecc.codes_set(output_bufr, 'typicalYearOfCentury', first_dt.year%100)
        ecc.codes_set(output_bufr, 'typicalMonth', first_dt.month)
        ecc.codes_set(output_bufr, 'typicalDay', first_dt.day)
        ecc.codes_set(output_bufr, 'typicalHour', first_dt.hour)
        ecc.codes_set(output_bufr, 'typicalMinute', first_dt.minute)

        # Set the data arrays
        set_arrays_for_dataframe(dataframe, output_bufr)

        # Finish the file
        ecc.codes_set(output_bufr, 'pack', 1)
        ecc.codes_write(output_bufr, output_filepath)

        ecc.codes_releast(output_bufr)


def set_arrays_for_dataframe(dataframe, output_bufr):
    # Longitude & Latitude
    ecc.codes_set_double_array(output_bufr, 'longitude',
                               dataframe['lon(deg.)'].to_numpy())
    ecc.codes_set_double_array(output_bufr, 'latitude',
                               dataframe['lat(deg.)'])

    # Height/Pressure
    # 1 Pa = 100 hPa
    ecc.codes_set_double_array(output_bufr, 'pressure',
                               dataframe['height(hPa)']/100)

    # Time
    ecc.codes_set_long_array(output_bufr, 'year',
                             dataframe['time(mjd)'].dt.year)
    ecc.codes_set_long_array(output_bufr, 'month',
                             dataframe['time(mjd)'].dt.month)
    ecc.codes_set_long_array(output_bufr, 'day',
                             dataframe['time(mjd)'].dt.day)
    ecc.codes_set_long_array(output_bufr, 'hour',
                             dataframe['time(mjd)'].dt.hour)
    ecc.codes_set_long_array(output_bufr, 'minute',
                             dataframe['time(mjd)'].dt.minute)

    # Wind speed and velocity
    u = dataframe['u (m/s)']
    v = dataframe['v (m/s)']

    wind_speed = sqrt(u^2 + v^2)
    wind_direction = 270 - atan(v/u)

    ecc.codes_set_double_array(output_bufr,
                               'windSpeed', wind_speed)
    ecc.codes_set_double_array(output_bufr,
                               'windDirection', wind_direction)

    # Quality Index
    # QI*100 = percent confidence
    # TODO: Confirm this is true
    # TODO: Does the QI apply directly like this?
    #           Do the u & v errors need combined?
    ecc.codes_set_double_array(output_bufr, 'windSpeed->percentConfidence',
                               100*dataframe['QI not using NWP'])
    ecc.codes_set_double_array(output_bufr, 'windDirection->percentConfidence',
                               100*dataframe['QI not using NWP'])

    # Satellite Zenith Angle
    ecc.codes_set_double_array(output_bufr, 'satelliteZenithAngle',
                               dataframe['satzenithangle(deg.)'])


# SCRIPT
def main():
    # These timezones are timezone naive to match those in jma_interface.
    start_dt = datetime(2010, 7, 30, 21)
    end_dt = datetime(2010, 7, 31, 3)

    data = get_wind_data("MTSAT-1R", "VIS", start_dt, end_dt)

    for key in data:
        print(key)
        for col in data[key]:
            print("\t", col)
        print()

    print("######")
    print(data['s']['time(mjd)'])
    print("######")
    print(data['s']['time(mjd)'].dt.year)
    print("######")
    print(data['s']['time(mjd)'][0])
    print("######")

    data_to_bufr(data, "data/test_out.bufr", "MTSAT-1R", "VIS")

if __name__ == "__main__":
    main()

