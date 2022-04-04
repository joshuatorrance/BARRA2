# This script was authored by Chun-Hse
# JT made a copy so that a couple of details could be tweaked.
"""
 To use, load the environment first

 . ~/env/setup_analysis3.sh
module use ~access/modules
module load eccodes


"""
import os, sys
import numpy as np
import glob
from datetime import datetime as dt
from datetime import timedelta as delt
import subprocess
import shutil

indir = sys.argv[1]
outdir = sys.argv[2]
files = glob.glob(os.path.join(indir, '*.bufr'))
files.sort()

# EDIT by JT, made filter_extract a full path and a variable
#  doing the same for filter_localtime
FILTER_DIR = "/g/data/hd50/chs548/barra2_shared_dev/bufr"

FILTER_6H_WINDOW_PATH = os.path.join(FILTER_DIR, "filter_extract_6hwindow")
FILTER_LOCALTIME_PATH = os.path.join(FILTER_DIR, "filter_localtime")

fid = open(FILTER_6H_WINDOW_PATH, 'r')
filter_text = fid.read()
fid.close()

print("files:", files)
for file in files:
    print("\n")
    print("Doing {:}".format(file))
    bn = os.path.basename(file)
    # EDIT by JT, my filenames are different e.g. IDM00096655-data.bufr
    #   tstr is replaced with the output time string for the output filename
    #   Let's replace the "data" with the timestring
    #tstr = bn[:12]
    tstr = "data"
    # EDIT by JT, as far as I can see this t is not used if the for loop below
    #  iterates.
    #t = dt.strptime(tstr, '%Y%m%d%H%M')
    
    # EDIT by JT, errors in try/catch being silenced, added e and print so
    #  I can see what's breaking.
    try:
        output = subprocess.run(["bufr_filter", FILTER_LOCALTIME_PATH, file], capture_output=True, text=True, check=True)
    except Exception as e:
        print(e)
        print("ERROR: bufr_filter error on {:}".format(file))
        continue

    validtimes = np.unique(output.stdout.strip().split('\n'))
    try:
        validtimes = [dt.strptime(t, '%Y%m%d%H%M') for t in validtimes]
    except Exception as e:
        print(e)
        validtimes2 = []
        for t in validtimes:
            try:
                validtimes2.append(dt.strptime(t, '%Y%m%d%H%M'))
            except Exception as e:
                print(e)
                continue
        validtimes = validtimes2

    windows = []
    for t in validtimes:
        if t.hour >= 21:
            # window centered at 0Z
            windows.append(dt(t.year, t.month, t.day) + delt(days=1))
        elif t.hour < 3:
            # window centered at 0Z
            windows.append(dt(t.year, t.month, t.day))
        elif t.hour >= 3 and t.hour < 9:
            # window centered at 6Z
            windows.append(dt(t.year, t.month, t.day, 6))
        elif t.hour >= 9 and t.hour < 15:
            # window cenerd at 12Z
            windows.append(dt(t.year, t.month, t.day, 12))
        elif t.hour >= 15 and t.hour < 21:
            # window centered at 18Z
            windows.append(dt(t.year, t.month, t.day, 18))

    windows = np.unique(windows)

    if len(windows) == 1:
        # don't need extraction
        t0 = windows[0]
        thisoutdir = os.path.join(outdir, "%04d" % t0.year, "%02d" % t0.month)
        if not os.path.exists(thisoutdir):
            os.makedirs(thisoutdir)
        outfile = os.path.join(thisoutdir, bn.replace(tstr, t0.strftime('%Y%m%d%H00')))
        tmpfile = outfile
        for i in range(1,10):
            if os.path.exists(tmpfile):
                tmpfile = outfile + "_%d" % i
            else:
                break
        outfile = tmpfile
        print("Copying to {:}".format(outfile))
        shutil.copy(file, outfile)
    else:
        for t0 in windows:
            t0i = t0 - delt(hours=3)
            t0j = t0 + delt(hours=2)
            start_year = str(t0i.year)
            start_month = str(t0i.month)
            start_day = str(t0i.day)
            start_hour = str(t0i.hour)

            end_year = str(t0j.year)
            end_month = str(t0j.month)
            end_day = str(t0j.day)
            end_hour = str(t0j.hour)

            thisoutdir = os.path.join(outdir, "%04d" % t0.year, "%02d" % t0.month)
            if not os.path.exists(thisoutdir):
                os.makedirs(thisoutdir)
            outfile = os.path.join(thisoutdir, bn.replace(tstr, t0.strftime('%Y%m%d%H00')))

            # EDIT by JT, if the output file already exists then skip ahead
            if os.path.exists(outfile):
                print(outfile, "already exists, skipping.")
                continue

            tmpfile = outfile
            for i in range(1,10):
                if os.path.exists(tmpfile):
                    tmpfile = outfile + "_%d" % i
                else:
                    break
            outfile = tmpfile

            update_filter = filter_text.replace('$START_YEAR', start_year).replace('$START_MONTH', start_month).replace('$START_DAY', start_day).replace('$START_HOUR', start_hour).replace('$END_YEAR', end_year).replace('$END_MONTH', end_month).replace('$END_DAY', end_day).replace('$END_HOUR', end_hour)

            filter_file = 'tmpfilter.%s' % t0.strftime('%Y%m%d%H')
            ff = open(filter_file, 'w')
            ff.write(update_filter)
            ff.close()

            # EDIT by JT, output to temporary file, then copy to final
            #    to allow for simpler recovery if interrupted
            temporary_outfile = outfile + ".temp"

            output = subprocess.run(["bufr_filter", filter_file, file, '-o', temporary_outfile], capture_output=True, text=True, check=True)

            shutil.move(temporary_outfile, outfile)

            print("Extracting to {:}".format(outfile))

            os.remove(filter_file)

print("SUCCESS: DONE!")
