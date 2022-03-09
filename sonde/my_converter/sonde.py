# This is a rewrite of Tan's Sonde to .converter. Rewriting in python3
# for posterity and my understanding.
#
# ecCodes3 may not be installed properly on NCI. Retreating to python2, attempting
# to keep it version agnostic.
#
# Documentation on the data format can be found at:
#   /g/data/hd50/barra2/data/obs/igra/doc/igra2-data-format.txt
#
# Author: Joshua Torrance

# IMPORTS
import numpy as np
import eccodes as ecc
import netCDF4
from scipy.constants import zero_Celsius, g as gravity
from datetime import datetime


# CLASSES
class SondeObservation:
    """
    A sonde observation, I presume this is a single sonde launch.

    A single observation should contain one or more levs
    """

    def __init__(self):
        # blk?
        self.station_blk = None
        self.station_number = None

        self.date_time = None

        # Latitude and longitude
        self.lat = None
        self.lon = None

        self.station_height = None

        # Levels (measurements at a given level/altitude/pressure)
        self.n_levels = None

        # Arrays - Initialise as None.
        self.pressure = None
        self.height = None
        self.air_temp = None
        # self.relative_humidity = None # Not used
        self.dew_point_temp = None
        self.wind_direction = None
        self.wind_speed = None

    def read_from_txt(self, txt_file):
        # Read the first line/header of the obs
        line = txt_file.readline()
        if line:
            self._read_first_line(line)
        else:
            return False

        # Now that we know the number of levels we can build the arrays
        self.pressure = np.zeros(self.n_levels)
        self.height = np.zeros(self.n_levels)
        self.air_temp = np.zeros(self.n_levels)
        #   self.relative_humidity = np.zeros(self.n_levels)    # not used
        self.dew_point_temp = np.zeros(self.n_levels)
        self.wind_direction = np.zeros(self.n_levels)
        self.wind_speed = np.zeros(self.n_levels)

        # Read the lines for each level
        for i in range(self.n_levels):
            line = txt_file.readline()

            if line:
                self._read_level(line, i)
            else:
                raise IOError("Reached end of file before end of observation.")

        return True

    def _read_first_line(self, line):
        """
        Process the first line of the block. It contains header-like info for
        a particular sounding.

        Example first line:
        #ASM00094120 1943 05 30 18 9999    7 ncdc6310          -124239  1308925
        
        According to the docs it should be (character columns don't match in
                                            the following line):
        #ID YEAR MONTH DAY HOUR HHmm NUM_LEVELS P_SRC NP_SRC LAT LON
        """
        self.station_blk = int(line[7:9])
        self.station_number = int(line[9:12])

        self.date_time = datetime(year=int(line[13:17]),
                                  month=int(line[18:20]),
                                  day=int(line[21:23]),
                                  hour=int(line[24:26]))

        self.n_levels = int(line[32:36])

        # TODO: Why 0.0001 (divide by 10,000)?
        self.lat = 0.0001 * int(line[55:62])
        self.lon = 0.0001 * int(line[63:71])

    def _read_level(self, line, level_index):
        """
        Process a line for each level
        
        Example level reading:
        21 -9999 101500A   10   256A  810 -9999   110    40

        LevelType ElapsedTime Pressure PFLAG GPH ZFLAG TEMP TFLAG RH DPDP WDIR WSPD
        """
        # Convenience vars
        missing_txt = SondeTXT.MISSING
        missing_ecc = ecc.CODES_MISSING_DOUBLE

        p = int(line[9:15])
        self.pressure[level_index] = p if p != missing_txt else missing_ecc

        # geometric height to geopotential height
        # TODO: Docs say this is already geopotential height...
        ht = int(line[16:21])
        self.height[level_index] = ht / gravity if ht != missing_txt else missing_ecc

        # 10C, convert to K
        #   "degrees C to tenths, e.g., 11 = 1.1 degrees C"
        #   so temperature given as milli-degrees, divide by ten and convert to Kelvin
        air_temp = int(line[22:27])
        self.air_temp[level_index] = 0.1 * int(line[22:27]) + zero_Celsius \
            if air_temp != missing_txt else missing_ecc

        # Relative humidity
        #  "percent to tenths, e.g., 11 = 1.1%"
        # self.relative_humidity[level_index] = 0.1 * int(line[28:33])

        # Dew point depression
        #   "degrees C to tenths, e.g., 11 = 1.1 degrees C"
        # TODO: Is dew point depression the same as dew point temperature?
        dp_temp = int(line[34:39])
        self.dew_point_temp[level_index] = 0.1 * dp_temp \
            if dp_temp != missing_txt else missing_ecc

        # Wind direction
        #  "degrees from north, 90 = east"
        wind_dir = int(line[40:45])
        self.wind_direction[level_index] = wind_dir \
            if wind_dir != missing_txt else missing_ecc

        # Wind speed
        #  "meters per second to tenths, e.g., 11 = 1.1 ms/1"
        wind_speed = int(line[46:51])
        self.wind_speed[level_index] = 0.1 * wind_speed \
            if wind_speed != missing_txt else missing_ecc

        # line[0:2] is the level type
        # First digit : Major level type indicator
        #               1 - Standard pressure level,
        #               2 - Other pressure level
        #               3 - Non-pressure level
        # Second digit: Minor level type indicator
        #               1 - Surface
        #               2 - Tropopause
        #               3 - Other
        # If this level has type "21" then it's at the surface and we
        #  can set the station height
        if line[0:2] == "21":
            self.station_height = self.height[level_index]


class SondeTXT:
    """
        This class represents original sonde data in txt format.
        Intended for use with BARRA2.
    """

    MISSING = -9999

    def __init__(self):
        self.observations = []

    def read(self, txt_file):
        """
        Read in txt file, one obs at a time

        Returns False if the end of the file has been reached, otherwise True.
        """

        while True:
            obs = SondeObservation()

            ret = obs.read_from_txt(txt_file)

            if ret:
                self.observations.append(obs)
            else:
                # obs.read_from_txt returns false when it sees the end of file
                break


class SondeNC:
    """
        bias-corrected sonde data in netcdf
    """
    MISSING = -999

    def __init__(self):
        self.n_days = 0
        self.n_levels = 0
        self.n_hours = 0

        self.year_month_day = None
        self.hours = None  # 2 launch times per day
        self.pressure = None
        self.air_temp = None
        self.bias = None

    def read(self, file):
        """
        read input netcdf data
        """
        with netCDF4.Dataset(file, 'r') as nc_in:
            # Assume latitude/longitude/altitude are correct.
            d_days_since_1900 = nc_in.variables['datum']  # 1943..20xx
            d_hours = nc_in.variables['hours']  # 0, 23
            d_pres = nc_in.variables['press']
            d_temperature = nc_in.variables['temperatures']
            d_bias = nc_in.variables['richbias']

            # Dimensions are hour x levels x date
            # Sondes are launched at the same time each day
            self.n_hours, self.n_levels, self.n_days = d_temperature.shape

            # Convert d_days_since_1900 to an array of datetimes
            t_units = nc_in.variables['datum'].units
            t_calendar_type = nc_in.variables['datum'].calendar
            t_value = netCDF4.num2date(d_days_since_1900, units=t_units, calendar=t_calendar_type)

            # Convert to numpy arrays
            self.year_month_day = np.array(d_days_since_1900)
            self.hours = np.array(d_hours)
            self.pressure = 100 * np.array(d_pres).astype(int)  # hPa
            self.air_temp = np.array(d_temperature)
            self.bias = np.array(d_bias)

            self.year_month_day.resize(self.n_days)
            for i in range(self.n_days):
                if self.year_month_day[i] != SondeNC.MISSING:
                    self.year_month_day[i] = 10000 * t_value[0][i].year + \
                                             100 * t_value[0][i].month + \
                                             t_value[0][i].day


class SondeBUFR:
    """
        output sonde data in BUFR for barra2
    """

    TEMP_SEQ = (1011, 1001, 1002, 4001, 4002, 4003, 4004, 4005, 5001,
                6001, 33003, 7001, 107000, 31002, 8001, 7004, 10009,
                12001, 12003, 11001, 11002, 2013, 2011, 2014, 11061,
                11062, 55017)

    def __init__(self, template_path, n_levels):
        self.pressure = [""] * n_levels
        self.height = [""] * n_levels
        self.air_temp = [""] * n_levels
        self.dew_point_temp = [""] * n_levels
        self.wind_direction = [""] * n_levels
        self.wind_speed = [""] * n_levels
        for i in range(n_levels):
            self.pressure[i] = '#' + str(i + 1) + '#pressure'
            self.height[i] = '#' + str(i + 1) + '#nonCoordinateGeopotentialHeight'
            self.air_temp[i] = '#' + str(i + 1) + '#airTemperature'
            self.dew_point_temp[i] = '#' + str(i + 1) + '#dewpointTemperature'
            self.wind_direction[i] = '#' + str(i + 1) + '#windDirection'
            self.wind_speed[i] = '#' + str(i + 1) + '#windSpeed'

        with open(template_path, 'r') as f_template:
            self.output_bufr = ecc.codes_bufr_new_from_file(f_template)

        self._year_month_day_index = 0

        ecc.codes_set(self.output_bufr, 'unpack', 1)

        # Fixed header values, do this once
        ecc.codes_set(self.output_bufr, 'edition', 3)
        ecc.codes_set(self.output_bufr, 'masterTableNumber', 0)
        ecc.codes_set(self.output_bufr, 'bufrHeaderCentre', 1)
        ecc.codes_set(self.output_bufr, 'dataCategory', 2)
        ecc.codes_set(self.output_bufr, 'dataSubCategory', 109)  # land temp
        ecc.codes_set(self.output_bufr, 'masterTablesVersionNumber', 26)
        ecc.codes_set(self.output_bufr, 'localTablesVersionNumber', 0)

        ecc.codes_set(self.output_bufr, 'typicalMinute', 0)
        ecc.codes_set(self.output_bufr, 'compressedData', 1)
        ecc.codes_set(self.output_bufr, 'numberOfSubsets', 1)

    def t_bias(self, hour_index, sonde_txt, sonde_nc):
        """
        Replace temperature with bias-corrected temperature from nc
        """
        for txt_level_index in range(sonde_txt.n_levels):
            for nc_level_index in range(sonde_nc.n_levels):
                if sonde_nc.pressure[nc_level_index] == sonde_txt.pressure[txt_level_index]:
                    if sonde_nc.air_temp[hour_index, nc_level_index, self._year_month_day_index] != sonde_nc.MISSING \
                          and sonde_nc.bias[hour_index, nc_level_index, self._year_month_day_index] != sonde_nc.MISSING:
                        ecc.codes_set(self.output_bufr,
                                      self.air_temp[txt_level_index],
                                      np.float64(sonde_nc.air_temp[hour_index,
                                                                   nc_level_index,
                                                                   self._year_month_day_index]
                                                 + sonde_nc.bias[hour_index,
                                                                 nc_level_index,
                                                                 self._year_month_day_index]))
                    break

    def write_temp(self, file_bufr, sonde_txt_obs, sonde_nc):
        """
        write out sonde for barra2
        """

        ecc.codes_set(self.output_bufr, 'unpack', 1)

        # TODO: 0.01 * year? What's going on here?
        #  2022 * 0.01 = 2022 / 100 = 20.22... a float and not the "typicalYearOfCentury"
        #  I suspect it should be (year % 100) to get two digit year,
        #  or just get get the two digit year from the datetime
        ecc.codes_set(self.output_bufr, 'typicalYearOfCentury', 0.01 * sonde_txt_obs.date_time.year)
        ecc.codes_set(self.output_bufr, 'typicalMonth', sonde_txt_obs.date_time.month)
        ecc.codes_set(self.output_bufr, 'typicalDay', sonde_txt_obs.date_time.day)
        ecc.codes_set(self.output_bufr, 'typicalHour', sonde_txt_obs.date_time.hour)

        ecc.codes_set(self.output_bufr,
                      'inputExtendedDelayedDescriptorReplicationFactor', sonde_txt_obs.n_levels)
        ecc.codes_set_array(self.output_bufr, 'unexpandedDescriptors', SondeBUFR.TEMP_SEQ)

        # ecc.codes_set(self.b_temp, 'shipOrMobileLandStationIdentifier', 'ASM000')
        ecc.codes_set(self.output_bufr, 'blockNumber', sonde_txt_obs.station_blk)
        ecc.codes_set(self.output_bufr, 'stationNumber', sonde_txt_obs.station_number)
        ecc.codes_set(self.output_bufr, 'year', sonde_txt_obs.date_time.year)
        ecc.codes_set(self.output_bufr, 'month', sonde_txt_obs.date_time.month)
        ecc.codes_set(self.output_bufr, 'day', sonde_txt_obs.date_time.day)
        ecc.codes_set(self.output_bufr, 'hour', sonde_txt_obs.date_time.hour)
        ecc.codes_set(self.output_bufr, 'minute', 0)
        ecc.codes_set(self.output_bufr, 'latitude', sonde_txt_obs.lat)
        ecc.codes_set(self.output_bufr, 'longitude', sonde_txt_obs.lon)
        ecc.codes_set(self.output_bufr, 'heightOfStation', sonde_txt_obs.station_height)

        for i in range(sonde_txt_obs.n_levels):
            ecc.codes_set(self.output_bufr, self.pressure[i], sonde_txt_obs.pressure[i])
            ecc.codes_set(self.output_bufr, self.height[i], sonde_txt_obs.height[i])
            ecc.codes_set(self.output_bufr, self.air_temp[i], sonde_txt_obs.air_temp[i])
            ecc.codes_set(self.output_bufr, self.dew_point_temp[i], sonde_txt_obs.dew_point_temp[i])
            ecc.codes_set(self.output_bufr, self.wind_direction[i], sonde_txt_obs.wind_direction[i])
            ecc.codes_set(self.output_bufr, self.wind_speed[i], sonde_txt_obs.wind_speed[i])

        # ecc.codes_set(self.b_temp, 'radiosondeType', t.sonde_type)

        # check if bias-corrected temp is available
        year_month_day = 10000 * sonde_txt_obs.date_time.year + \
            100 * sonde_txt_obs.date_time.month + \
            sonde_txt_obs.date_time.day

        print(year_month_day, sonde_nc.year_month_day[self._year_month_day_index])

        while sonde_nc.year_month_day[self._year_month_day_index] < year_month_day:
            self._year_month_day_index += 1

        if year_month_day == sonde_nc.year_month_day[self._year_month_day_index]:
            for hour_index in range(sonde_nc.n_hours):
                if sonde_txt_obs.date_time.hour == sonde_nc.hours[hour_index, self._year_month_day_index]:
                    self.t_bias(hour_index, sonde_txt_obs, sonde_nc)
                    break

            # What's this if up to? Can I use a for else clause instead? Or inside the loop.
            if hour_index == 1:
                self._year_month_day_index += 1

        # Avoid unpacking self.b_temp the next time
        ecc.codes_set(self.output_bufr, 'pack', 1)
        ecc.codes_write(self.output_bufr, file_bufr)

    def close(self):
        ecc.codes_release(self.output_bufr)
