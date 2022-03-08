# Utilities for AMSR2
#
# Joshua Torrance (jt4085)

# IMPORTS
from h5py import File as hdfFile
from shutil import copyfile
from datetime import datetime, timedelta, timezone
from os import remove as delete_file
from os.path import splitext, exists
from numpy import array
from math import floor, ceil


# FUNCTIONS
# Binning Utils
def get_bins(start_dt, end_dt, offset_sec=3*60*60, bin_size_sec=6*60*60):
    # Return a list of dictionaries describing the bins
    bin_list = []

    cur_bin_start_dt = align_bin(start_dt,
                                 offset_sec=offset_sec,
                                 bin_size_sec=bin_size_sec,
                                 edge="start")

    bin_size_td = timedelta(seconds=bin_size_sec)
    while True:
        cur_bin_mid_dt = cur_bin_start_dt + bin_size_td/2
        cur_bin_end_dt = cur_bin_start_dt + bin_size_td

        bin_dict = {
            "start": cur_bin_start_dt,
            "mid": cur_bin_mid_dt,
            "end": cur_bin_end_dt
        }

        bin_list.append(bin_dict)

        cur_bin_start_dt = cur_bin_end_dt

        if cur_bin_start_dt >= end_dt:
            break

    return bin_list


def align_bin(dt, offset_sec=3*60*60, bin_size_sec=6*60*60, edge="start"):
    # Timestamps give seconds since midnight, 1st Jan 1970
    #  Ensure datetime is NOT timezone naive to avoid headaches
    # Apply offset since bins don't start at midnight
    # Divide by bin duration, floor/ceil to get start/end of bin
    # Multiply by bin duration and reapply the offset to
    #  get the timestamp for the bin edge
    if edge == "start":
        func = floor
    elif edge == "end":
        func = ceil
    else:
        raise ValueError("align_bin: edge must be \"start\" or \"end\"")

    timestamp = dt.timestamp()
    aligned_timestamp = func((timestamp - offset_sec) / bin_size_sec) * bin_size_sec + offset_sec

    dt = datetime.fromtimestamp(aligned_timestamp, tz=timezone.utc)

    return dt


def build_regexs_for_ftp_from_datetimes(start_dt, end_dt, prefix='GW1AM2_', include_hour_before=False):
    # Builds regexes to match the file structure of the JAXA G-Portal FTP server
    # Structure is something like YYYY/MM/GW1AM2_YYYYMMDDhhmm_xxxx_L1SGBTBR_2220220.h5

    # Filenames look like - GW1AM2_201912010040_180A_L1SGBTBR_2220220.h5
    # ^ static, _ dynamic   ^^^^^^^____________^____^^^^^^^^^^^^^^^^^^^^

    # include_hour_before tacks on the hour before the start of start_dt
    #   This is done to include the data between start_dt and the start of the first file after start_dt
    #   This function relies on JAXA data files having less than an hour between them
    if include_hour_before:
        start_dt = start_dt - timedelta(hours=1)

    # Years dir regex
    year_regex = "|".join([str(x) for x in
                           range(start_dt.year,
                                 max(start_dt.year + 1, end_dt.year + 1))])

    # Months dir regex
    months = []
    month = start_dt.month
    while True:
        months.append(month)

        if month == end_dt.month:
            break

        # Increment month and deal with months wrapping at the end of the year
        month = max(1, (month + 1) % 13)

    # Convert months to a set to remove duplicates
    month_regex = "|".join([str(x) for x in set(months)])

    # Files regex - for now assume that datetimes are aligned to the hour
    dt = start_dt
    file_regex = []
    while True:
        # Escape { with {{, escape \ with \\
        file_regex.append("{:4}{:02d}{:02d}{:02d}\\d{{2}}".format(dt.year, dt.month, dt.day, dt.hour))

        dt += timedelta(hours=1)

        # We don't want files whose names match the end dt.
        # E.g. if the end id 2019-01-01 00:00 we don't want files that match 2019010100xx
        if dt >= end_dt:
            break

    file_regex = prefix + '|'.join(file_regex)

    return year_regex, month_regex, file_regex


# HDF Utils
def _get_split_index(hdf_filepath, split_point_dt,
                     margin_td=timedelta(seconds=3)):
    with hdfFile(hdf_filepath) as hdf:
        # Attribute is stored as a one element numpy array for some reason.
        # Datetime string has a Z on the end which doesn't match the
        #  ISO format so replace it with +00:00
        start_dt = datetime.fromisoformat(hdf.attrs['ObservationStartDateTime'][0]
                                          .replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(hdf.attrs['ObservationEndDateTime'][0]
                                        .replace('Z', '+00:00'))

        # Check that the split is between the start and end.
        # margin give the number of seconds of grace allowed between the
        #  split point and the start/end datetimes. There's at least one bin
        #  where the end_dt is less than 2 seconds from the split spoint.
        if (start_dt - split_point_dt) < margin_td and \
            (split_point_dt - end_dt) < margin_td:
            # Convert scantime to timestamps (i.e. seconds since 1970-01-01T00:00 UTC)
            scan_time = hdf['Scan Time']
            timestamps = (scan_time - scan_time[0]) + start_dt.timestamp()
 
            # Determine the index of the first element after split_point_dt
            split_index = (timestamps > split_point_dt.timestamp()).argmax()

            return split_index
        else:
            raise ValueError("hdf_util._get_split_index: split_point_dt "
                             "outside HDF file's datetime range.")


def _filter_amsr2_hdf(hdf_filepath, split_index, mode='before'):
    with hdfFile(hdf_filepath, 'r+') as hdf:
        # Get the length of scan time so we can ensure we're
        # splitting on the correct dimension.
        t_length = hdf['Scan Time'].size

        # Build the indices for the split
        if mode == 'before':
            indices = range(split_index)
        elif mode == 'after':
            indices = range(split_index, t_length)
        else:
            raise ValueError("filter_amsr2_hdf: mode must be \"before\" or \"after\"")

        # Split every dataset in the file
        for key in hdf:
            dataset = hdf[key]

            # Determine which axis to split along
            split_axis = None
            for i, axis_len in enumerate(dataset.shape):
                if axis_len == t_length:
                    # Assume this is the axis to split along
                    # If another axis has the same length we might be in trouble.
                    split_axis = i
                    break

            # Convert the h5py dataset to a numpy array and
            # split it
            arr = array(dataset).take(indices, axis=split_axis)

            # Resize the existing dataset to the new array size
            dataset.resize(arr.shape)

            # Write the new array to the dataset
            dataset[...] = arr


def split_hdf_at_datetime(hdf_filepath, split_point_dt, output_filepaths=None):
    # Determine the index of the split_point
    split_index = _get_split_index(hdf_filepath, split_point_dt)

    # Make copies of the input file.
    if not output_filepaths:
        # If no output is defined append _before/_after onto the input filepath
        file_p, file_ext = splitext(hdf_filepath)
        f_before = file_p + "_before" + file_ext
        f_after = file_p + "_after" + file_ext 
    else:
        f_before = output_filepaths[0]
        f_after = output_filepaths[1]

    # Ensure the output files do not have the same filepath
    if f_before == f_after:
        # Clean up copied files
        if hdf_filepath != f_before and exists(f_before):
            delete_file(f_before)

        if hdf_filepath != f_after and exists(f_after):
            delete_file(f_after)

        raise ValueError("amsr2_util.split_hdf_at_datetime: output files have the same filepath.")

    # Copy the files
    # Copy both files before splitting in case either of the copies has the same filepath as the original
    if f_before:
        if hdf_filepath != f_before:
            copyfile(hdf_filepath, f_before)

    if f_after:
        if hdf_filepath != f_after:
            copyfile(hdf_filepath, f_after)

    # Filter out the data before/after the split point on each copy.
    if f_before:
        _filter_amsr2_hdf(f_before, split_index, mode='before')

    if f_after:
        _filter_amsr2_hdf(f_after, split_index, mode='after')
