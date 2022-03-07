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

        # Levs (levels? measurements at a given level/altitude/pressure?)
        self.n_levs = None

        # Lev arrays
        self.pressure = None
        self.ht = None
        self.air_temp = None
        # Not used
        # self.relative_humidity = None
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

        # Now that we know the number of levs we can build the arrays
        self.pressure = np.zeros(self.n_levs)
        self.ht = np.zeros(self.n_levs)
        self.air_temp = np.zeros(self.n_levs)
        #   self.relative_humidity = np.zeros(self.n_levs)    # not used
        self.dew_point_temp = np.zeros(self.n_levs)
        self.wind_direction = np.zeros(self.n_levs)
        self.wind_speed = np.zeros(self.n_levs)

        # Read the lines for each lev
        for i in range(self.n_levs):
            line = txt_file.readline()

            if line:
                self._read_levs(line, i)
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
                                            the following line'):
        #ID YEAR MONTH DAY HOUR HHmm NUM_LEVELS P_SRC NP_SRC LAT LON
        """
        self.station_blk = int(line[7:9])
        self.station_number = int(line[9:12])

        self.date_time = datetime(year=int(line[13:17]),
                                  month=int(line[18:20]),
                                  day=int(line[21:23]),
                                  hour=int(line[24:26]))

        self.n_levs = int(line[32:36])

        # TODO: Why 0.0001 (divide by 10,000)?
        self.lat = 0.0001 * int(line[55:62])
        self.lon = 0.0001 * int(line[63:71])

    def _read_levs(self, line, lev_index):
        """
        Process a line for each level
        
        Example level reading:
        21 -9999 101500A   10   256A  810 -9999   110    40
        LevelType1 ElapsedTime Pressure PFLAG GPH ZFLAG TEMP TFLAG RH DPDP WDIR WSPD
        """
        # TODO: update this to not split, data format defines columsn not
        #       whitespace separated strings
        # get rid of the "A", where from '(a2,i5,i6,6i5)' ???
        line = line.replace("A", " ")
        x = line.split()
        for i in range(len(x)):
            if int(x[i]) == SondeTXT.MISSING:
                x[i] = ecc.CODES_MISSING_DOUBLE

        print "#####################"
        print line

        p = int(line[9:15])
        self.pressure[lev_index] = p if p!=SondeTXT.MISSING else ecc.CODES_MISSING_DOUBLE


        # geometric height to geopotential height
        # TODO: Docs say this is already geopotential height
        ht = int(line[16:21])
        self.ht[lev_index = ht / gravity if ht!=SondeTXT.MISSING else ecc.CODES_MISSING_DOUBLE

        # 10C, convert to K
        if self.air_temp[lev_index] != ecc.CODES_MISSING_DOUBLE:
            self.air_temp[lev_index] = 0.1 * int(x[4]) + zero_Celsius

        # self.relative_humidity[lev_index] = int(x[5])
        self.dew_point_temp[lev_index] = int(x[6])
        self.wind_direction[lev_index] = int(x[7])
        self.wind_speed[lev_index] = int(x[8])

        # line[0:2] is the level type
        # First digit : Major level type indicator
        #               1 - Standard pressure level,
        #               2 - Other pressure level
        #               3 - Non-pressure level
        # Second digit: Minor level type indicator
        #               1 - Surface
        #               2 - Tropopause
        #               3 - Other
        if line[0:2] == "21":
            self.station_height = self.ht[lev_index]


class SondeTXT:
    """
        This class represents original sonde data in txt format.
        Intended for use with BARRA2.
    """

    MISSING = -9999

    def __init__(self):
        self.levs = []

    def read(self, txt_file):
        """
        Read in txt file, one obs at a time

        Returns False if the end of the file has been reached, otherwise True.
        """

        while True:
            lev = SondeObservation()

            ret = lev.read_from_txt(txt_file)

            if ret:
                self.levs.append(lev)
            else:
                # lev.read_from_txt returns false when it sees the end of file
                break


class SondeNC:
    """
        bias-corrected sonde data in netcdf
    """
    MISSING = -999

    def __init__(self):
        self.n_days = 0
        self.n_levs = 0
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

            self.n_hours, self.n_levs, self.n_days = d_temperature.shape

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

    def __init__(self, template_path, n_levs):
        self.pressure = [""] * n_levs
        self.ht = [""] * n_levs
        self.air_temp = [""] * n_levs
        self.dew_point_temp = [""] * n_levs
        self.wind_direction = [""] * n_levs
        self.wind_speed = [""] * n_levs
        for i in range(n_levs):
            self.pressure[i] = '#' + str(i + 1) + '#pressure'
            self.ht[i] = '#' + str(i + 1) + '#nonCoordinateGeopotentialHeight'
            self.air_temp[i] = '#' + str(i + 1) + '#airTemperature'
            self.dew_point_temp[i] = '#' + str(i + 1) + '#dewpointTemperature'
            self.wind_direction[i] = '#' + str(i + 1) + '#windDirection'
            self.wind_speed[i] = '#' + str(i + 1) + '#windSpeed'

        with open(template_path, 'r') as f_template:
            # I think f_template is a template bufr file?
            self.b_temp = ecc.codes_bufr_new_from_file(f_template)

        self._year_month_day_index = 0

        ecc.codes_set(self.b_temp, 'unpack', 1)

        # fixed header values, do this once
        ecc.codes_set(self.b_temp, 'edition', 3)
        ecc.codes_set(self.b_temp, 'masterTableNumber', 0)
        ecc.codes_set(self.b_temp, 'bufrHeaderCentre', 1)
        ecc.codes_set(self.b_temp, 'dataCategory', 2)
        ecc.codes_set(self.b_temp, 'dataSubCategory', 109)  # land temp
        ecc.codes_set(self.b_temp, 'masterTablesVersionNumber', 26)
        ecc.codes_set(self.b_temp, 'localTablesVersionNumber', 0)

        ecc.codes_set(self.b_temp, 'typicalMinute', 0)
        ecc.codes_set(self.b_temp, 'compressedData', 1)
        ecc.codes_set(self.b_temp, 'numberOfSubsets', 1)

    def t_bias(self, hour_index, sonde_txt, sonde_nc):
        """
        replace temp with bias-corrected temp from nc
        """
        for txt_lev_index in range(sonde_txt.n_levs):
            for nc_lev_index in range(sonde_nc.n_levs):
                if sonde_nc.pressure[nc_lev_index] == sonde_txt.pressure[txt_lev_index]:
                    if sonde_nc.air_temp[hour_index, nc_lev_index, self._year_month_day_index] != sonde_nc.MISSING \
                            and sonde_nc.bias[hour_index, nc_lev_index, self._year_month_day_index] != sonde_nc.MISSING:
                        ecc.codes_set(self.b_temp,
                                      self.air_temp[txt_lev_index],
                                      np.float64(sonde_nc.air_temp[hour_index, nc_lev_index, self._year_month_day_index]
                                                 + sonde_nc.bias[hour_index, nc_lev_index, self._year_month_day_index]))
                    break

    def write_temp(self, file_bufr, sonde_txt_obs, sonde_nc):
        """
        write out sonde for barra2
        """

        ecc.codes_set(self.b_temp, 'unpack', 1)

        # TODO: 0.01 * year? What's going on here?
        #  2022 * 0.01 = 2022 / 100 = 20.22... a float and not the "typicalYearOfCentury"
        ecc.codes_set(self.b_temp, 'typicalYearOfCentury', 0.01 * sonde_txt_obs.date_time.year)
        ecc.codes_set(self.b_temp, 'typicalMonth', sonde_txt_obs.date_time.month)
        ecc.codes_set(self.b_temp, 'typicalDay', sonde_txt_obs.date_time.day)
        ecc.codes_set(self.b_temp, 'typicalHour', sonde_txt_obs.date_time.hour)

        ecc.codes_set(self.b_temp,
                      'inputExtendedDelayedDescriptorReplicationFactor', sonde_txt_obs.n_levs)
        ecc.codes_set_array(self.b_temp, 'unexpandedDescriptors', SondeBUFR.TEMP_SEQ)

        # ecc.codes_set(self.b_temp, 'shipOrMobileLandStationIdentifier', 'ASM000')
        ecc.codes_set(self.b_temp, 'blockNumber', sonde_txt_obs.station_blk)
        ecc.codes_set(self.b_temp, 'stationNumber', sonde_txt_obs.station_number)
        ecc.codes_set(self.b_temp, 'year', sonde_txt_obs.date_time.year)
        ecc.codes_set(self.b_temp, 'month', sonde_txt_obs.date_time.month)
        ecc.codes_set(self.b_temp, 'day', sonde_txt_obs.date_time.day)
        ecc.codes_set(self.b_temp, 'hour', sonde_txt_obs.date_time.hour)
        ecc.codes_set(self.b_temp, 'minute', 0)
        ecc.codes_set(self.b_temp, 'latitude', sonde_txt_obs.lat)
        ecc.codes_set(self.b_temp, 'longitude', sonde_txt_obs.lon)
        ecc.codes_set(self.b_temp, 'heightOfStation', sonde_txt_obs.station_height)

        for i in range(sonde_txt_obs.n_levs):
            ecc.codes_set(self.b_temp, self.pressure[i], sonde_txt_obs.pressure[i])
            ecc.codes_set(self.b_temp, self.ht[i], sonde_txt_obs.ht[i])
            ecc.codes_set(self.b_temp, self.air_temp[i], sonde_txt_obs.air_temp[i])
            ecc.codes_set(self.b_temp, self.dew_point_temp[i], sonde_txt_obs.dew_point_temp[i])
            ecc.codes_set(self.b_temp, self.wind_direction[i], sonde_txt_obs.wind_direction[i])
            ecc.codes_set(self.b_temp, self.wind_speed[i], sonde_txt_obs.wind_speed[i])

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
        ecc.codes_set(self.b_temp, 'pack', 1)
        ecc.codes_write(self.b_temp, file_bufr)

    def close(self):
        ecc.codes_release(self.b_temp)
