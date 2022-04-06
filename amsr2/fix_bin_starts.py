# AMSR-2 data for BARRA2
#
# Data files for at the start of each bin where the original file had been
# split did not have the meta-data for the start time updated correctly.
# This script goes through the hdf archive and updates said meta-data.
#
# Author: Joshua Torrance

# IMPORTS
from glob import glob
from os.path import join, basename
from datetime import datetime, timedelta
from h5py import File, string_dtype


# PARAMETERS
DATA_DIR = "/scratch/hd50/jt4085/amsr2/hdf"

ARCHIVE_DT_FORMAT = "%Y%m%dT%H%MZ"

HDF_START_KEY = "ObservationStartDateTime"
HDF_DT_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

HDF_OLD_START_KEY = "OriginalObservationStartDateTime"


# METHODS
def set_start_time_in_hdf(hdf_filename, new_start_time_dt):
    with File(hdf_filename, 'r+') as hdf:
        # Get the current value
        # It's an array for some reason
        cur_start_str = hdf.attrs[HDF_START_KEY][0]
        cur_start_dt = datetime.strptime(cur_start_str, HDF_DT_FORMAT)

        if cur_start_dt != new_start_time_dt:
            print("\t\t\tSaving old start time...", end="")

            hdf.attrs[HDF_OLD_START_KEY] = [cur_start_str]

            print("done.")

            print("\t\t\tUpdating start time...", end="")
            new_start_str = new_start_time_dt.strftime(HDF_DT_FORMAT)
            
            # Too many digits for microseconds - HH:MM:123456Z
            new_start_str = new_start_str[:-4] + 'Z'

            # Set the key to the new value
            # As a list since the value is an ndarray for some reason
            # Need to convert to ASCII string for the fortran converter
            hdf.attrs.create(HDF_START_KEY, [new_start_str],
                             dtype=string_dtype(encoding='ascii'))

            print("done.")


        else:
            print("\t\t\tStart time already correct.")


# SCRIPT
def main():
    years = glob(join(DATA_DIR, "*"))
    years.sort()
    for y_dir in years:
        y = basename(y_dir)

        print(y)

        months = glob(join(y_dir, "*"))
        months.sort()
        for m_dir in months:
            m = basename(m_dir)

            print("\t", m)

            dts = glob(join(m_dir, "*"))
            dts.sort()
            for dt_dir in dts:
                dt_str = basename(dt_dir)
                dt = datetime.strptime(dt_str, ARCHIVE_DT_FORMAT)
                start_bin_dt = dt - timedelta(hours=3)
                
                print("\t\t", dt_str)

                # HDF filenames are similar to:
                #  GW1AM2_201502282100_140A_L1SGBTBR_2220220.h5
                file_str = "*{y:04}{mon:02}{d:02}{h:02}{min:02}*.h5".format(
                    y=start_bin_dt.year,
                    mon=start_bin_dt.month,
                    d=start_bin_dt.day,
                    h=start_bin_dt.hour,
                    min=start_bin_dt.minute) 

                start_edge_hdfs = glob(join(dt_dir, file_str))

                if len(start_edge_hdfs) > 0:
                    set_start_time_in_hdf(start_edge_hdfs[0], start_bin_dt)


if __name__ == "__main__":
    # Testing
    main()
