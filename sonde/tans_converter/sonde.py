import sys
import numpy as np
import eccodes as ecc
import netCDF4

class sonde_txt:
    'original sonde data in txt for barra2'

    NLEVS = 100

    stn_blk = stn_num = 0
    yy = mm = dd = hh = 0
    sth_ht = nlevs = 0
    lat = lon = 0.0

    pres = np.zeros(NLEVS, dtype=int)
    ht = np.zeros(NLEVS)
    temp = np.zeros(NLEVS)
#   rh = np.zeros(NLEVS)    # not used
    dewpt = np.zeros(NLEVS)
    dir = np.zeros(NLEVS)
    spd = np.zeros(NLEVS)

#============================================================================
    def first_line(self, l):
        """
        process first line
        """
# WRITE(udatafile, '('#',a11,i4,3i2.2,i4.4,i4,a9,i7,i8,i5,a2,a5,a20,a1,6a2)') &
# cStnid, iCurYear, iCurMon, iCurDay, iCurHour, iRTime, iNewNLvls, cClouds, &
# iLat, iLon, iElev, cObsType, cSondeType, cSerialNum, cSerialNumType, &
# cCorP, cCorZ, cCorT, cCorH, cCorD, cCorW

# #ASM00094120 1943 05 30 18 9999    7 ncdc6310          -124239  1308925

        x = l.split()
        self.stn_blk = int(x[0][7:9])
        self.stn_num = int(x[0][9:12])
        self.yy = int(x[1])
        self.mm = int(x[2])
        self.dd = int(x[3])
        self.hh = int(x[4])
        self.nlevs = int(x[6])
        self.lat = 0.0001 * int(x[8])
        self.lon = 0.0001 * int(x[9])

#============================================================================
    def read_levs(self, l, ilev):
        """
        process line for each lev
        """

# WRITE(udatafile,'(a2,i5,i6,6i5)') &
# cLvlTypes(i), iETimes(i), iPressures(i), iHeights(i), iTemps(i), &
# iRelHums(i), iDewDeps(i), iWDirs(i), iWSpeeds(i)

# 21 -9999 101000A   27   229A-9999 -9999 -9999 -9999

        K = 273.15
        G = 9.81

        # get rid of the "A", where from '(a2,i5,i6,6i5)' ???
        l = l.replace("A", " ")
        x = l.split()
        for i in range(len(x)):
            if int(x[i]) == -9999:
                x[i] = ecc.CODES_MISSING_DOUBLE

        self.levtype = x[0]
        self.pres[ilev] = int(x[2])

        # geometric height to geopotential height
        if self.ht[ilev] != ecc.CODES_MISSING_DOUBLE:
            self.ht[ilev] = int(x[3]) / G

        # 10C, convert to K
        # values to be overwritten from bias-corrected netcdf file
        if self.temp[ilev] != ecc.CODES_MISSING_DOUBLE:
            self.temp[ilev] = 0.1 * int(x[4]) + K

#       self.rh[ilev] = int(x[5])
        self.dewpt[ilev] = int(x[6])
        self.dir[ilev] = int(x[7])
        self.spd[ilev] = int(x[8])

        if self.levtype == "21": # sfc
            self.stn_ht = self.ht[ilev]

#============================================================================
    def read_txt(self, f):
        """
        read in txt file, one obs at a time
        """

        l = f.readline()
        if l:
            # if l[0] == "#":
            self.first_line(l)
            for i in range(self.nlevs):
                l = f.readline()
                self.read_levs(l, i)
            return 1
        else:
            return 0


#============================================================================
#============================================================================
class sonde_nc:
    'bias-corrected sonde data in netcdf'

    MISSING = -999

    nt = nlevs = nhours = 0

    ymd  = np.zeros(1, dtype=int)
    hh   = np.zeros( (1, 1), dtype=int ) # 2 launch times per day
    pres = np.zeros(1, dtype=int)
    temp = np.zeros( (1, 1, 1) )
    bias = np.zeros( (1, 1, 1) )

#============================================================================
    def read(self, file):
        """
        read input netcdf data
        """

        nc_in = netCDF4.Dataset(file, 'r')

        # assume correct lat/lon/altitude
        d_ymd  = nc_in.variables['datum']  # 1943..20xx
        d_hh   = nc_in.variables['hours'] # 0, 23
        d_pres = nc_in.variables['press']
        d_temp = nc_in.variables['temperatures']
        d_bias = nc_in.variables['richbias']

        self.hh, self.nlevs, self.nt = d_temp.shape

        t_units = nc_in.variables['datum'].units
        t_cal   = nc_in.variables['datum'].calendar
        tval    = netCDF4.num2date(d_ymd, units = t_units, calendar = t_cal)

        # convert to numpy array
        self.ymd  = np.array(d_ymd)
        self.hh   = np.array(d_hh)
        self.pres = 100 * np.array(d_pres).astype(int) # hPa
        self.temp = np.array(d_temp)
        self.bias = np.array(d_bias)

        self.ymd.resize(self.nt)
        for i in range(self.nt):
            if self.ymd[i] != -999:
                self.ymd[i] = 10000*tval[0][i].year + 100*tval[0][i].month + \
                              tval[0][i].day
#       print(self.ymd[0], self.ymd[1])
#       print(self.hh[0, 0], self.hh[1, 0], self.hh[0, 1], self.hh[1, 1],
#             self.hh[0, 2], self.hh[1, 2])

        del d_ymd
        del d_hh
        del d_pres
        del d_temp
        del d_bias

        nc_in.close()


#============================================================================
#============================================================================
class sonde_bfr:
    'output sonde data in BUFR for barra2'

    f_temp = b_temp = 0
    temp_seq = ( 1011, 1001, 1002, 4001, 4002, 4003, 4004, 4005, 5001,
                 6001, 33003, 7001, 107000, 31002, 8001, 7004, 10009,
                 12001, 12003, 11001, 11002, 2013, 2011, 2014, 11061,
                 11062, 55017 )
    pres = ht = temp = dewpt = dir = spd = [""]

#============================================================================
    def __init__(self, t):
        """
        class init
        """

        self.pres = [""] * t.NLEVS
        self.ht = [""] * t.NLEVS
        self.temp = [""] * t.NLEVS
        self.dewpt = [""] * t.NLEVS
        self.dir = [""] * t.NLEVS
        self.spd = [""] * t.NLEVS
        for i in range(t.NLEVS):
            self.pres[i] = '#' + str(i+1) + '#pressure'
            self.ht[i] = '#' + str(i+1) + '#nonCoordinateGeopotentialHeight'
            self.temp[i] = '#' + str(i+1) + '#airTemperature'
            self.dewpt[i] = '#' + str(i+1) + '#dewpointTemperature'
            self.dir[i] = '#' + str(i+1) + '#windDirection'
            self.spd[i] = '#' + str(i+1) + '#windSpeed'

        self.f_temp = open("../test_data/template.bufr", 'r')
        self.b_temp = ecc.codes_bufr_new_from_file(self.f_temp)

        ecc.codes_set(self.b_temp, 'unpack', 1)

        # fixed header values, do this once
        ecc.codes_set(self.b_temp, 'edition', 3)
        ecc.codes_set(self.b_temp, 'masterTableNumber', 0)
        ecc.codes_set(self.b_temp, 'bufrHeaderCentre', 1)
        ecc.codes_set(self.b_temp, 'dataCategory', 2)
        ecc.codes_set(self.b_temp, 'dataSubCategory', 109) # land temp
        ecc.codes_set(self.b_temp, 'masterTablesVersionNumber', 26)
        ecc.codes_set(self.b_temp, 'localTablesVersionNumber', 0)

        ecc.codes_set(self.b_temp, 'typicalMinute', 0)
        ecc.codes_set(self.b_temp, 'compressedData', 1)
        ecc.codes_set(self.b_temp, 'numberOfSubsets', 1)

#============================================================================
    def tbias(self, h, t, nc, idx):
        """
        replace temp with bias-corrected temp from nc
        """

        for lt in range(t.nlevs):
            for l in range(nc.nlevs):
                if nc.pres[l] == t.pres[lt]:
                    if (nc.temp[h, l, idx] != nc.MISSING and
                        nc.bias[h, l, idx] != nc.MISSING):
                        ecc.codes_set(self.b_temp, self.temp[lt],
                          np.float64(nc.temp[h, l, idx] + nc.bias[h, l, idx]))
                    break

#============================================================================
    def write_temp(self, f, t, nc, idx):
        """
        write out sonde for barra2
        """

        ecc.codes_set(self.b_temp, 'unpack', 1)

        ecc.codes_set(self.b_temp, 'typicalYearOfCentury', 0.01 * t.yy)
        ecc.codes_set(self.b_temp, 'typicalMonth', t.mm)
        ecc.codes_set(self.b_temp, 'typicalDay', t.dd)
        ecc.codes_set(self.b_temp, 'typicalHour', t.hh)

        ecc.codes_set(self.b_temp,
            'inputExtendedDelayedDescriptorReplicationFactor', t.nlevs)
        ecc.codes_set_array(self.b_temp, 'unexpandedDescriptors', self.temp_seq)

        # ecc.codes_set(self.b_temp, 'shipOrMobileLandStationIdentifier', 'ASM000')
        ecc.codes_set(self.b_temp, 'blockNumber', t.stn_blk)
        ecc.codes_set(self.b_temp, 'stationNumber', t.stn_num)
        ecc.codes_set(self.b_temp, 'year', t.yy)
        ecc.codes_set(self.b_temp, 'month', t.mm)
        ecc.codes_set(self.b_temp, 'day', t.dd)
        ecc.codes_set(self.b_temp, 'hour', t.hh)
        ecc.codes_set(self.b_temp, 'minute', 0)
        ecc.codes_set(self.b_temp, 'latitude', t.lat)
        ecc.codes_set(self.b_temp, 'longitude', t.lon)
        ecc.codes_set(self.b_temp, 'heightOfStation', t.stn_ht)

        for i in range(t.nlevs):
            ecc.codes_set(self.b_temp, self.pres[i], t.pres[i])
            ecc.codes_set(self.b_temp, self.ht[i], t.ht[i])
            ecc.codes_set(self.b_temp, self.temp[i], t.temp[i])
            ecc.codes_set(self.b_temp, self.dewpt[i], t.dewpt[i])
            ecc.codes_set(self.b_temp, self.dir[i], t.dir[i])
            ecc.codes_set(self.b_temp, self.spd[i], t.spd[i])

        # ecc.codes_set(self.b_temp, 'radiosondeType', t.sonde_type)

        # check if bias-corrected temp is available
        ymd = 10000*t.yy + 100*t.mm + t.dd
        print(ymd, nc.ymd[idx])
        while nc.ymd[idx] < ymd:
            idx += 1
        if ymd == nc.ymd[idx]:
            for h in range(2):
                if t.hh == nc.hh[h, idx]:
                    self.tbias(h, t, nc, idx) 
                    break
            if h == 1:
                idx += 1

        # avoid unpacking self.b_temp the next time
        ecc.codes_set(self.b_temp, 'pack', 1)
        ecc.codes_write(self.b_temp, f)

        # keep this for next obs
        # ecc.codes_release(self.b_temp)
        # self.f_temp.close()

        return idx
