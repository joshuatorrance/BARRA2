# Author: Chun-Hsu
# Original location:
# /g/data/hd50/chs548/barra2_shared_dev/jma_wind/jma_interface.py 
# or
# https://code.metoffice.gov.uk/svn/utils/access/branches/dev/Share/barra2_shared_dev/jma_wind
#

"""
 Directory: Himawari8_algorithm > [GMS-5|GOES-9|MTSAT-1R|MTSAT-2].tar.gz > yyyymm > dd > hh > [a|f|s]
* [a|f|s] means
 a: Full disk (for GMS-5 and GOES-9)
 [f|s]: Northern(f) and Southern(s) hemisphere (for MTSAT-1R and MTSAT-2)

File name: AMVDAT_CONSORTIUM_[VIS|IR1|IR2|IR3|IR4].CSV.gz
 The csv files are compressed (gzip).

Central wavelength:
 VIS: 0.675 um
 IR1: 10.8 um
 IR2: 12.0 um
 IR3: 6.75 um
 IR4: 3.75 um

Term: June 1995 to July 2015
 GMS-5: 13/Jun/1995-22/May/2003
 GOES-9: 22/May/2003-28/Jun/2005
 MTSAT-1R: 01/Jun/2005-31/Jul/2010
 MTSAT-2: 01/Jul/2010-31/Jul/2015
"""

import os, sys
import numpy as np
import pandas as pd
import glob
from datetime import datetime as dt
from datetime import timedelta as delt
import random
import gzip
from netCDF4 import num2date, date2num

input_dir = '/scratch/hd50/barra2/data/obs/'
input_dir = '/scratch/hd50/jt4085/jma_wind/'
infile_template = input_dir + '$instrument/%Y%m/%d/%H/$coverage/*$CHANNEL*.gz'
_platforms = ['MTSAT-2', 'MTSAT-1R']
_time_units = 'days since 1858-11-17 00:00:00'
_channels = ['VIS', 'IR1', 'IR2', 'IR3', 'IR4']
_tempdir = '/scratch/hd50/%s' % os.environ['USER']

def find_closest_basetime(t):
    # Data is organised as 0, 6, 12, and 18
    # EDIT by JT: 5.99 is not closest to 0... it's closest to 6
    if t.hour >= 0 and t.hour < 3:
        bt = dt(t.year, t.month, t.day, 0)
    elif t.hour >= 3 and t.hour < 9:
        bt = dt(t.year, t.month, t.day, 6)
    elif t.hour >= 9 and t.hour < 15:
        bt = dt(t.year, t.month, t.day, 12)
    elif t.hour >= 15 and t.hour < 21:
        bt = dt(t.year, t.month, t.day, 18)
    else:
        bt = dt(t.year, t.month, t.day, 0) + delt(days=1)

    return bt

def find_all_files(platform, channel, tstart, tend):
    """
    Return all the files available for given platform, between the given time period.
    find_all_files('MTSAT-1R', 'VIS', dt(2010, 7, 30, 21), dt(2010, 7, 31, 3))
    """

    assert platform in _platforms, "ERROR: Current implemented for {:} only".format(_platforms)
    assert channel in _channels, "ERROR: Current implemented for {:} only".format(_channels)

    infile_templ = infile_template.replace('$instrument', platform).replace('$CHANNEL', channel)
    file_start = find_closest_basetime(tstart)
    file_end = find_closest_basetime(tend)
    file_datetimes = pd.date_range(file_start, file_end, freq='6H')

    files_included = {}
    for coverage in ['s', 'f', 'a']:
        files_subset = []
        for datetime in file_datetimes:
            #print("Doing {:}".format(datetime))
            infiles = glob.glob(datetime.strftime(infile_templ).replace('$coverage', coverage))
            infiles.sort()
            #print(infiles)
            files_subset += infiles

        files_included[coverage] = files_subset

    return files_included

def get_wind_data(platform, channel, tstart, tend):
    """
    Return the data frame for the given platform, channel and time window.
    get_wind_data('MTSAT-1R', 'VIS', dt(2010, 7, 30, 21), dt(2010, 7, 31, 3))
    """
    infiles = find_all_files(platform, channel, tstart, tend)
    data = {}
    for coverage in infiles.keys():
        if len(infiles[coverage]) == 0:
            continue

        df_from_each_file = (pd.read_csv(f, compression='gzip') for f in infiles[coverage])
        df   = pd.concat(df_from_each_file, ignore_index=True)

        # EDIT by JT: occaasionally the CSVs have no data rows
        if len(df) == 0:
            continue
        time = num2date(df['time(mjd)'].values, _time_units)
        time = [dt(t.year, t.month, t.day, t.hour, t.minute, t.second) for t in time]
        # covnert to datetime objects
        df['time(mjd)'] = time
        # truncate df based on time window
        itime = np.where(np.greater_equal(time, tstart) & np.less_equal(time, tend))[0]

        # EDIT by JT: check itime has elements
        if len(itime) == 0:
            continue

        if itime[0] > 0:
            df = df.truncate(before=itime[0])
        if itime[-1] < len(time)-1:
            df = df.truncate(after=itime[-1])
        
        data[coverage] = df

    return data


