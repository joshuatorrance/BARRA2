#!/bin/python3

from ftplib import FTP
from shutil import copyfile
from os import remove as delete_file, makedirs
from os.path import basename, exists, splitext, dirname, join
from re import search, sub
from subprocess import call
from sys import stdout
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool

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
DATA_DIR = 'data/2019'

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


def get_list_of_files(ftp, path_str):
    results = ftp.mlsd(path_str)

    # Filter the list of files to remove any non-files, . and ..
    file_list = [x[0] for x in results
                 if x[1]['type'] == 'file' and
                     not (x[0] == '.' or x[0] == '..')]

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


# SCRIPT
if __name__ == "__main__":
    if False:
        with FTP(JAXA_FTP) as ftp_connection:
            ret = ftp_connection.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)

            y_regex = "2019"
            m_regex = "12"
            list_of_files = jaxa_get_full_list_of_filepaths(ftp_connection, FTP_DIR,
                                                            year_regex=y_regex,
                                                            month_regex=m_regex)

        process_files(list_of_files, max_files=MAX_FILES)#, interval_hours=6)

    if True:
        from amsr2_util import get_bins, build_regexs_for_ftp_from_datetimes

        # For a time range
        #   determine the bins
        #   grab the data for those bins
        #       split edge files as required

        start_dt = datetime.fromisoformat("2019-12-01T00:00+00:00")
        end_dt = datetime.fromisoformat("2019-12-03T00:00+00:00")

        # DTs to test year/month boundary on first bin
        #start_dt = datetime.fromisoformat("2019-12-31T23:00+00:00")
        #end_dt = datetime.fromisoformat("2020-12-03T00:00+00:00")

        bins = get_bins(start_dt, end_dt)#, bin_size_sec=60*60*24*356.25*4.80)

        # Temporary download directory
        temp_dir = join(DATA_DIR, "temp")

        # The last file in the previous bin should overhang the start of the next bin
        last_file_of_prev_bin = None
        for b in bins:
            # For each bin grab the files from JAXA to fill the bin.
            # This likely includes a file whose name/timestamp is before
            # the bin window.
            print("Start:", b['start'])
            print("End:  ", b['end'])

            # TODO: Finish the implementation.

            if not last_file_of_prev_bin:
                # Go get the last file of the previous bin
                pass

                # Split the file along the bin's start datetime

            # Get the files within the bin
            year_regex, month_regex, file_regex = \
                build_regexs_for_ftp_from_datetimes(b['start'], b['end'])

            print(year_regex)
            print(month_regex)
            print(file_regex)

            # Split the last file across the bin end datetime

            # Organise the files into the archive

            break



