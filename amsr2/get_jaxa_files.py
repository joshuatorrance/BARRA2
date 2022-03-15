#!/bin/python3

from argparse import ArgumentParser, ArgumentTypeError
from ftplib import FTP
from shutil import copyfile, move as move_file
from os import remove as delete_file, rmdir, makedirs
from os.path import basename, exists, splitext, dirname, join
from re import search, sub
from subprocess import call
from sys import stdout
from datetime import datetime, timedelta, timezone
from multiprocessing.dummy import Pool
from functools import partial

from amsr2_util import get_bins, build_regexs_for_ftp_from_datetimes, \
    split_hdf_at_datetime, get_observation_limit_from_file

# PARAMETERS
JAXA_FTP = 'ftp.gportal.jaxa.jp'

# Credentials for JAXA
# Generic password for the JAXA service so I can get away
# with a plain text password.
# This is NOT APPROPRIATE for non-generic passwords.
FTP_USERNAME = 'joshuatorrance'
FTP_PASSWORD = 'anonymous'

FTP_DIR = '/standard/GCOM-W/GCOM-W.AMSR2/L1B/2/'

FTP_MAX_CONNECTIONS = 5

CONVERSION_SCRIPT = './run_converter.sh'

# Datetime format in JAXA filenames (201801010007 -> yyyymmddhhss
JAXA_DT_FORMAT = "%Y%m%d%H%M"

# Local Data directory
DATA_DIR = 'data'

# Archive datetime format for final directories etc.
ARCHIVE_DT_FORMAT = "%Y%m%dT%H%MZ"

# Input command line argument datetime format
COMMANDLINE_DT_FORMAT = "%Y%m%dT%H%M"

# File type extensions
HDF_EXT = '.h5'
BUFR_EXT = '.bufr'

# File limit, use for testing, set to None for no limit.
MAX_FILES = 100


# FUNCTIONS
def get_file(path_str, output_dir=DATA_DIR,
             output_filename=None, overwrite=False):
    # Grab the filename from the path if output_filename is not specified.
    filename = basename(path_str) if not output_filename else output_filename

    filepath = join(output_dir, filename)

    # TODO: Check the size of the file here too to ensure it's intact.

    # Don't overwrite the file if it already exists unless overwrite==True
    if not exists(filepath) or overwrite:
        makedirs(dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as fp:
            # Opening a new FTP connection here rather than re-using a connection
            # supports multi-threading.
            with FTP(JAXA_FTP) as ftp:
                ftp.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)

                try:
                    ftp.retrbinary('RETR ' + path_str, fp.write)
                except Exception as e:
                    # If something goes wrong clean up any incomplete file.
                    print("Interrupted while downloading file:", filepath)
                    print("Cleaning up incomplete file...")
                    fp.close()
                    delete_file(filepath)

                    raise e

    return filepath


def get_files(ftp_file_paths, output_dir, n_threads=FTP_MAX_CONNECTIONS):
    with Pool(n_threads) as p:
        func = partial(get_file, output_dir=output_dir)

        file_paths = p.map(func, ftp_file_paths)

        p.close()
        p.join()

    return file_paths


def get_list_of_files(ftp, path_str):
    results = ftp.mlsd(path_str)

    # Filter the list of files to remove any non-files, . and ..
    file_list = [x[0] for x in results
                 if x[1]['type'] == 'file' and not (x[0] == '.' or x[0] == '..')]

    return file_list


def get_list_of_dirs(ftp, path_str):
    results = ftp.mlsd(path=path_str)

    # Filter the list of directories to remove any non-directories, . and ..
    dir_list = [x[0] for x in results
                if x[1]['type'] == 'dir' and not (x[0] == '.' or x[0] == '..')]

    return dir_list


def jaxa_get_full_list_of_filepaths(ftp, starting_dir_path,
                                    year_regex=None, month_regex=None, filename_regex=None):
    # Get a list of years, sort them, and filter on the regex if supplied.
    list_of_year_dirs = get_list_of_dirs(ftp, starting_dir_path)
    list_of_year_dirs.sort()
    if year_regex:
        list_of_year_dirs = \
            [y for y in list_of_year_dirs if search(year_regex, y)]

    full_list_of_filepaths = []
    for year in list_of_year_dirs:
        path = starting_dir_path + year

        # Get a list of months, sort them, and filter on the regex if supplied.
        list_of_month_dirs = get_list_of_dirs(ftp, path)
        list_of_month_dirs.sort()
        if month_regex:
            list_of_month_dirs = \
                [d for d in list_of_month_dirs if search(month_regex, d)]

        for month in list_of_month_dirs:
            # Don't use os.path.join here as we're talking to FTP
            path2 = path + '/' + month

            filenames = get_list_of_files(ftp, path2)
            filenames.sort()

            if filename_regex:
                filenames = \
                    [f for f in filenames if search(filename_regex, f)]

            for filename in filenames:
                # Don't use os.path.join here as we're talking to FTP
                files_path = path2 + '/' + filename

                full_list_of_filepaths.append(files_path)

    return full_list_of_filepaths


def get_date_string_from_filename(filename):
    # Just look for a 12-digit number in the filename (YYYYMMDDHHmm)
    x = search("\\d{12}", filename)

    if x:
        return x.group()

def get_datetime_from_filename(filename):
    date_str = get_date_string_from_filename(filename)

    return datetime.strptime(date_str + "+0000", JAXA_DT_FORMAT + "%z")


def convert_hdf_to_bufr(in_file_path, out_file_path=None):
    if exists(CONVERSION_SCRIPT):
        try:
            call([CONVERSION_SCRIPT, '-i ' + in_file_path, '-o ' + out_file_path])
        except Exception as e:
            # If we're interrupted or something goes wrong
            # clean up and pass on the exception.
            print("Conversion interrupted by", e)
            print("Cleaning up incomplete .bufr.")
            delete_file(out_file_path)

            # Re-raise the exception to pass it on.
            raise e
    else:
        # TODO: This is a placeholder function to be replaced
        #  with call to the conversion program.
        if not out_file_path:
            root, ext = splitext(in_file_path)
            out_file_path = root + ".bufr"

        copyfile(in_file_path, out_file_path)


def process_file(i, ftp_file_path, data_d=DATA_DIR):
    ret_str = "Processing file {}: {}\n".format(i+1, basename(ftp_file_path))

    # Local filenames
    hdf_file_path = join(data_d, basename(ftp_file_path))
    hdf_root, hdf_ext = splitext(hdf_file_path)
    bufr_file_path = hdf_root + ".bufr"

    ret_str += "\tDir: " + data_d + "\n"
    ret_str += "\tHDF: " + basename(hdf_file_path) + "\n"

    if not exists(bufr_file_path):
        # Get the file from the JAXA FTP server.
        ret_str += "\tDownloading HDF..."
        stdout.flush()
        get_file(ftp_file_path, output_dir=data_d)
        ret_str += "done.\n"

        # Convert the HDF file to a BUFR file.
        ret_str += "\tConverting to BUFR..."
        stdout.flush()
        convert_hdf_to_bufr(hdf_file_path, bufr_file_path)
        ret_str += "Done.\n"

        # Delete the HDF file.
        ret_str += "\tDeleting HDF..."
        stdout.flush()
        delete_file(hdf_file_path)
        ret_str += "Done.\n"
    else:
        ret_str += "\tBUFR file already exists.\n"

    return ret_str


def generate_file_path_bin_dir_tuples(ftp_file_paths, data_d=DATA_DIR,
                                      start_time_dt=None, interval_hours=None,
                                      max_files=None):
    tuples = []

    file_count = 0
    current_bin_start_dt = start_time_dt
    bin_dir = data_d
    interval_td = timedelta(hours=interval_hours) if interval_hours else None
    for i, f in enumerate(ftp_file_paths):
        if max_files and file_count >= max_files:
            break
        else:
            file_count += 1

        f_dt = datetime.strptime(get_date_string_from_filename(f), JAXA_DT_FORMAT)

        def get_bin_dir_path(dt):
            bin_dir_name = dt.isoformat().replace(":", "").replace("-", "")
            # Remove colons and hyphens from the dir name to avoid complications.
            return join(data_d, bin_dir_name)

        if current_bin_start_dt:
            if current_bin_start_dt + interval_td < f_dt:
                # We've left the bin, start a new one.
                # Allow for multiple bin increments in case there's a gap in files
                while current_bin_start_dt + interval_td < f_dt:
                    current_bin_start_dt += interval_td

                bin_dir = get_bin_dir_path(current_bin_start_dt)
        elif interval_hours:
            # No start time supplied, use time of the first file.
            # Align the starting bin to the hour
            current_bin_start_dt = f_dt.replace(minute=0, second=0, microsecond=0)

            bin_dir = get_bin_dir_path(current_bin_start_dt)

        tuples.append((i, f, bin_dir))

    return tuples


def process_files(ftp_file_paths, data_d=DATA_DIR,
                  start_time_dt=None, interval_hours=None,
                  max_files=None, n_threads=FTP_MAX_CONNECTIONS):
    """
    Takes a list of files on the FTP server, downloads them one-by-one, processing
    them into BUFRs and organising them into interval_hours directories if provided.

    :param ftp_file_paths: List of filepaths on the ftb server, ordered by time.
    :param data_d: Directory to place the downloaded and proccessed files into.
    :param start_time_dt: Datetime to start the binning at, leave as None to just use the time of the first file.
    :param interval_hours: Time interval to separate the file directories.
    :param max_files: Maximum number of files to process, use for testing.
    :param n_threads: Number of threads to multiprocess with.
    :return:
    """
    # Generate a list of (ftp_file_path, bin_dir) tuples.
    file_path_bin_dir_tuples = generate_file_path_bin_dir_tuples(ftp_file_paths, data_d,
                                                                 start_time_dt, interval_hours,
                                                                 max_files)

    with Pool(n_threads) as p:
        for tup in file_path_bin_dir_tuples:
            p.apply_async(process_file, tup,
                          callback=lambda result: print(result))

        p.close()
        p.join()


def get_hdfs_between_datetimes(start_dt, end_dt, output_dir=DATA_DIR, ftp_dir=FTP_DIR):
    # Generate the bin dictionaries (list of {start_dt, end_dt, mid_dt})
    bins = get_bins(start_dt, end_dt)

    # Temporary download directory
    temp_dir = join(output_dir, "temp")

    # The last file in the previous bin should overhang the start of the next bin
    # Handle the first bin separately
    last_file_of_prev_bin = None
    for b in bins:
        # For each bin grab the files from JAXA to fill the bin.
        # This likely includes a file whose name/timestamp is before
        # the bin window.
        print("Bin middle:", b['mid'])

        # Generate the regexes for the bin
        year_regex, month_regex, file_regex = \
            build_regexs_for_ftp_from_datetimes(b['start'], b['end'],
                                                include_hour_before=last_file_of_prev_bin is None)

        # Grab the files from the FTP server
        with FTP(JAXA_FTP) as ftp_connection:
            ftp_connection.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)

            # Get the list of files that match the regexes.
            files = jaxa_get_full_list_of_filepaths(ftp_connection, ftp_dir,
                                                    year_regex=year_regex, month_regex=month_regex,
                                                    filename_regex=file_regex)

        # Download each of the files
        local_file_paths = get_files(files, temp_dir)
        local_file_paths.sort()

        if len(local_file_paths) == 0:
            # No files found, must be an empty bin.
            print("\tBin is empty, skipping.")

            if last_file_of_prev_bin:
                delete_file(last_file_of_prev_bin)

            last_file_of_prev_bin = None

            continue

        # Get the last file of the previous bin
        if not last_file_of_prev_bin:
            # Ensure the end of the file is after the start of the bin
            file_end_dt = get_observation_limit_from_file(local_file_paths[0], "End")
            while file_end_dt < b['start']:
                # File ends before the start of the bin, delete the file.
                delete_file(local_file_paths[0])

                # Remove it from the file list.
                local_file_paths = local_file_paths[1:]

                # Check the next file.
                file_start_dt = get_datetime_from_filename(basename(local_file_paths[0]))
                file_end_dt = get_observation_limit_from_file(local_file_paths[0], "End")
                
            # Only split the file if is starts before the start of the bin.
            #   Use the filename for the start_dt as, at time of writing, the split
            #   hdf methods do not update the hdf.attrs start/end labels.
            file_start_dt = get_datetime_from_filename(basename(local_file_paths[0]))
            if file_start_dt < b['start']:
                # File begins before the start of the bin as expected.
                # File ends after the start of the bin as expected.
                last_file_of_prev_bin = local_file_paths[0]

                def tidy_first_bin_edge(original_filepath, bin_edge_dt):
                    # Name the new file with the starting datetime
                    dt_str = get_date_string_from_filename(basename(original_filepath))
                    new_file = original_filepath.replace(dt_str, bin_edge_dt.strftime(JAXA_DT_FORMAT))

                    # Split the file along the bin's start datetime
                    split_hdf_at_datetime(original_filepath, bin_edge_dt, output_filepaths=(None, new_file))

                    return new_file

                try:
                    new_filename = tidy_first_bin_edge(last_file_of_prev_bin, b['start'])
                except ValueError:
                    # Sometimes there are two files in the hour before the start of the bin.
                    # This will generate a ValueError
                    # Delete the too-early file and remove it from the local file list

                    # This shouldn't happen anymore, due to checking the start/end times.
                    # TODO: Remove this portion.
                    delete_file(last_file_of_prev_bin)
                    local_file_paths = local_file_paths[1:]

                    # Reassign the last_file_of_prev_bin
                    last_file_of_prev_bin = local_file_paths[0]

                    new_filename = tidy_first_bin_edge(last_file_of_prev_bin, b['start'])

                # Delete the original file
                delete_file(last_file_of_prev_bin)

                # Update the file list
                local_file_paths[0] = new_filename
            else:
                # First file starts after the start of the bin, don't need to
                # split the file or alter the filename.
                pass
        else:
            local_file_paths = [last_file_of_prev_bin] + local_file_paths

        # Split the last file across the bin end datetime
        last_file = local_file_paths[-1]
        
        file_end_dt = get_observation_limit_from_file(last_file, "End")
        if file_end_dt > b['end']:
            # If the end of the last file is after the end of the bin then split it.
            date_str = get_date_string_from_filename(basename(last_file))

            new_filename_before = last_file
            new_filename_after = last_file.replace(date_str, b['end'].strftime(JAXA_DT_FORMAT))

            split_hdf_at_datetime(last_file, b['end'],
                                output_filepaths=(new_filename_before, new_filename_after))

            # Update the prev_bin file as it will be used in the next iteration
            # Note that it's not in the local_file_paths list.
            last_file_of_prev_bin = new_filename_after
        else:
            # End of the last file is before the end of the bin.
            last_file_of_prev_bin = None

        # Archive directory
        archive_dir = join(output_dir,
                           "{:04d}".format(b['mid'].year),
                           "{:02d}".format(b['mid'].month),
                           b['mid'].strftime(ARCHIVE_DT_FORMAT))

        # Organise the files into the archive
        # Bins are organised about the middle of the bin
        makedirs(archive_dir, exist_ok=True)
        for f in local_file_paths:
            dest = join(archive_dir, basename(f))
            if exists(dest):
                delete_file(dest)

            move_file(f, dest)

    # Clean up the temp directory
    if last_file_of_prev_bin:
        delete_file(last_file_of_prev_bin)

    rmdir(temp_dir)


# MAIN SCRIPT
def parse_args():
    # Date time func to parse datetime command line args.
    def valid_date(s):
        try:
            # Parse the input string
            return datetime.strptime(s + "+0000", COMMANDLINE_DT_FORMAT + "%z")
        except ValueError:
            msg = "not a valid date: {0!r}".format(s)
            raise ArgumentTypeError(msg)

    parser = ArgumentParser(prog="get_jaxa_files.py",
                            description="This script gets HDF files from JAXA for a particular observation "
                                        "between a start datetime and end datetime and places them in binned "
                                        "directories."
                                        "\n\n"
                                        "Author: Joshua Torrance")

    parser.add_argument("-o", "--output-dir", nargs="?", required=True,
                        help="Output directory for the binned data.")
    parser.add_argument("-s", "--start", nargs="?", required=True, type=valid_date,
                        help="Start UTC datetime to grab data for. Will be aligned to the bin edge before it. "
                             "Use the format " + COMMANDLINE_DT_FORMAT)
    parser.add_argument("-e", "--end", nargs="?", required=True, type=valid_date,
                        help="End UTC datetime to grab data for. Will be aligned to the bin edge after it. "
                             "Use the format " + COMMANDLINE_DT_FORMAT)
    parser.add_argument("--ftp-dir", nargs="?", default=FTP_DIR,
                        help="The JAXA FTP dir to grab data from, determines the observation. "
                             "Defaults to AMSR-2.")

    return parser.parse_args()


def main():
    args = parse_args()

    start_dt = args.start
    end_dt = args.end
    ftp_dir = args.ftp_dir
    output_dir = args.output_dir

    get_hdfs_between_datetimes(start_dt=start_dt, output_dir=output_dir,
                               end_dt=end_dt, ftp_dir=ftp_dir)


if __name__ == "__main__":
    if False:
        with FTP(JAXA_FTP) as ftp_con:
            ftp_con.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)

            y_regex = "2019"
            m_regex = "12"
            list_of_files = jaxa_get_full_list_of_filepaths(ftp_con, FTP_DIR,
                                                            year_regex=y_regex,
                                                            month_regex=m_regex)

        process_files(list_of_files, max_files=MAX_FILES)#, interval_hours=6)

    main()
