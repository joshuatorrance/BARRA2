# This is a rewrite of Tan's Sonde to .converter. Rewriting in python3
# for posterity and my understanding.
#
# ecCodes3 may not be installed properly on NCI. Retreating to python2, attempting
# to keep it version agnostic.
#
# Author: Joshua Torrance

# IMPORTS
import numpy as np
import eccodes as ecc
import netCDF4
from scipy.constants import zero_Celsius, g as gravity
from datetime import datetime


# CLASSES
class SondeTXT:
    """
        This class represents original sonde data in txt format.
        Intended for use with BARRA2.
    """

    MISSING = -9999

    N_LEVS = 100

    def __init__(self):
        # blk?
        self.station_blk = None
        self.station_number = None

        self.date_time = None

        self.n_levs = 0

        # Latitude and longitude
        self.lat = 0.0
        self.lon = 0.0

        # Pressure
        self.pressure = np.zeros(SondeTXT.N_LEVS, dtype=int)
        self.ht = np.zeros(SondeTXT.N_LEVS)

        # Air temperature
        self.air_temp = np.zeros(SondeTXT.N_LEVS)

        # Relative humidity
    #   self.relative_humidity = np.zeros(SondeTXT.N_LEVS)    # not used

        # Dew point temperature
        self.dew_point_temp = np.zeros(SondeTXT.N_LEVS)

        # Wind direction and speed
        self.wind_direction = np.zeros(SondeTXT.N_LEVS)
        self.wind_speed = np.zeros(SondeTXT.N_LEVS)

        self.lev_type = None

        self.station_height = None

    def first_line(self, line):
        """
        Process the first line of the block. It contains header-like info.

        Example first line:
        #ASM00094120 1943 05 30 18 9999    7 ncdc6310          -124239  1308925
        """
        # WRITE(udatafile, '('#',a11,i4,3i2.2,i4.4,i4,a9,i7,i8,i5,a2,a5,a20,a1,6a2)') &
        # cStnid, iCurYear, iCurMon, iCurDay, iCurHour, iRTime, iNewNLvls, cClouds, &
        # iLat, iLon, iElev, cObsType, cSondeType, cSerialNum, cSerialNumType, &
        # cCorP, cCorZ, cCorT, cCorH, cCorD, cCorW

        x = line.split()
        self.station_blk = int(x[0][7:9])
        self.station_number = int(x[0][9:12])

        self.date_time = datetime(year=int(x[1]),
                                  month=int(x[2]),
                                  day=int(x[3]),
                                  hour=int(x[4]))

        self.n_levs = int(x[6])
        self.lat = 0.0001 * int(x[8])
        self.lon = 0.0001 * int(x[9])

    def read_levs(self, line, ilev):
        """
        process line for each lev
        """

        # WRITE(udatafile,'(a2,i5,i6,6i5)') &
        # cLvlTypes(i), iETimes(i), iPressures(i), iHeights(i), iTemps(i), &
        # iRelHums(i), iDewDeps(i), iWDirs(i), iWSpeeds(i)

        # 21 -9999 101000A   27   229A-9999 -9999 -9999 -9999

        # get rid of the "A", where from '(a2,i5,i6,6i5)' ???
        line = line.replace("A", " ")
        x = line.split()
        for i in range(len(x)):
            if int(x[i]) == SondeTXT.MISSING:
                x[i] = ecc.CODES_MISSING_DOUBLE

        self.lev_type = x[0]
        self.pressure[ilev] = int(x[2])

        # geometric height to geopotential height
        if self.ht[ilev] != ecc.CODES_MISSING_DOUBLE:
            self.ht[ilev] = int(x[3]) / gravity

        # 10C, convert to K
        # values to be overwritten from bias-corrected netcdf file
        if self.air_temp[ilev] != ecc.CODES_MISSING_DOUBLE:
            self.air_temp[ilev] = 0.1 * int(x[4]) + zero_Celsius

#       self.rh[ilev] = int(x[5])
        self.dew_point_temp[ilev] = int(x[6])
        self.wind_direction[ilev] = int(x[7])
        self.wind_speed[ilev] = int(x[8])

        if self.lev_type == "21":  # sfc
            self.station_height = self.ht[ilev]

    def read_txt(self, txt_file):
        """
        Read in txt file, one obs at a time

        Returns False if the end of the file has been reached, otherwise True.
        """

        line = txt_file.readline()
        if line:
            self.first_line(line)
            for i in range(self.n_levs):
                line = txt_file.readline()
                self.read_levs(line, i)

            return True
        else:
            return False


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

    def __init__(self, template_path):
        self.pressure = [""] * SondeTXT.N_LEVS
        self.ht = [""] * SondeTXT.N_LEVS
        self.air_temp = [""] * SondeTXT.N_LEVS
        self.dew_point_temp = [""] * SondeTXT.N_LEVS
        self.wind_direction = [""] * SondeTXT.N_LEVS
        self.wind_speed = [""] * SondeTXT.N_LEVS
        for i in range(SondeTXT.N_LEVS):
            self.pressure[i] = '#' + str(i+1) + '#pressure'
            self.ht[i] = '#' + str(i+1) + '#nonCoordinateGeopotentialHeight'
            self.air_temp[i] = '#' + str(i+1) + '#airTemperature'
            self.dew_point_temp[i] = '#' + str(i+1) + '#dewpointTemperature'
            self.wind_direction[i] = '#' + str(i+1) + '#windDirection'
            self.wind_speed[i] = '#' + str(i+1) + '#windSpeed'

        with open(template_path, 'r') as f_template:
            # I think f_template is a template bufr file?
            self.b_temp = ecc.codes_bufr_new_from_file(f_template)

        self.idx = 0

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
                    if sonde_nc.air_temp[hour_index, nc_lev_index, self.idx] != sonde_nc.MISSING \
                            and sonde_nc.bias[hour_index, nc_lev_index, self.idx] != sonde_nc.MISSING:
                        ecc.codes_set(self.b_temp,
                                      self.air_temp[txt_lev_index],
                                      np.float64(sonde_nc.air_temp[hour_index, nc_lev_index, self.idx]
                                                 + sonde_nc.bias[hour_index, nc_lev_index, self.idx]))
                    break

    def write_temp(self, file_bufr, sonde_txt, sonde_nc):
        """
        write out sonde for barra2
        """

        ecc.codes_set(self.b_temp, 'unpack', 1)

        # TODO: 0.01 * year? What's going on here?
        #  2022 * 0.01 = 2022 / 100 = 20.22... a float and not the "typicalYearOfCentury"
        ecc.codes_set(self.b_temp, 'typicalYearOfCentury', 0.01 * sonde_txt.date_time.year)
        ecc.codes_set(self.b_temp, 'typicalMonth', sonde_txt.date_time.month)
        ecc.codes_set(self.b_temp, 'typicalDay', sonde_txt.date_time.day)
        ecc.codes_set(self.b_temp, 'typicalHour', sonde_txt.date_time.hour)

        ecc.codes_set(self.b_temp,
                      'inputExtendedDelayedDescriptorReplicationFactor', sonde_txt.n_levs)
        ecc.codes_set_array(self.b_temp, 'unexpandedDescriptors', SondeBUFR.TEMP_SEQ)

        # ecc.codes_set(self.b_temp, 'shipOrMobileLandStationIdentifier', 'ASM000')
        ecc.codes_set(self.b_temp, 'blockNumber', sonde_txt.station_blk)
        ecc.codes_set(self.b_temp, 'stationNumber', sonde_txt.station_number)
        ecc.codes_set(self.b_temp, 'year', sonde_txt.date_time.year)
        ecc.codes_set(self.b_temp, 'month', sonde_txt.date_time.month)
        ecc.codes_set(self.b_temp, 'day', sonde_txt.date_time.day)
        ecc.codes_set(self.b_temp, 'hour', sonde_txt.date_time.hour)
        ecc.codes_set(self.b_temp, 'minute', 0)
        ecc.codes_set(self.b_temp, 'latitude', sonde_txt.lat)
        ecc.codes_set(self.b_temp, 'longitude', sonde_txt.lon)
        ecc.codes_set(self.b_temp, 'heightOfStation', sonde_txt.station_height)

        for i in range(sonde_txt.n_levs):
            ecc.codes_set(self.b_temp, self.pressure[i], sonde_txt.pressure[i])
            ecc.codes_set(self.b_temp, self.ht[i], sonde_txt.ht[i])
            ecc.codes_set(self.b_temp, self.air_temp[i], sonde_txt.air_temp[i])
            ecc.codes_set(self.b_temp, self.dew_point_temp[i], sonde_txt.dew_point_temp[i])
            ecc.codes_set(self.b_temp, self.wind_direction[i], sonde_txt.wind_direction[i])
            ecc.codes_set(self.b_temp, self.wind_speed[i], sonde_txt.wind_speed[i])

        # ecc.codes_set(self.b_temp, 'radiosondeType', t.sonde_type)

        # check if bias-corrected temp is available
        year_month_day = 10000 * sonde_txt.date_time.year + \
            100 * sonde_txt.date_time.month + \
            sonde_txt.date_time.day

        print(year_month_day, sonde_nc.year_month_day[self.idx])

        while sonde_nc.year_month_day[self.idx] < year_month_day:
            self.idx += 1

        if year_month_day == sonde_nc.year_month_day[self.idx]:
            for hour_index in range(sonde_nc.n_hours):
                if sonde_txt.date_time.hour == sonde_nc.hours[hour_index, self.idx]:
                    self.t_bias(hour_index, sonde_txt, sonde_nc)
                    break

            # What's this if up to? Can I use a for else clause instead? Or inside the loop.
            if hour_index == 1:
                self.idx += 1

        # Avoid unpacking self.b_temp the next time
        ecc.codes_set(self.b_temp, 'pack', 1)
        ecc.codes_write(self.b_temp, file_bufr)

    def close(self):
        ecc.codes_release(self.b_temp)

