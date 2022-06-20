# Script to generate diagnostic figures comparing BARRA2 to AWAP data.
#
# Use with:
# python3 awap_diag.py -o /OUTPUT/DIR/PATH -d YYYYMMDD
#
# Uses this module:
# conda/analysis3-22.04

# IMPORTS
from argparse import ArgumentParser, ArgumentTypeError
from datetime import datetime, time, timedelta
from glob import glob
from os import mkdir
from os.path import join
from shutil import unpack_archive
from tempfile import TemporaryDirectory
from warnings import catch_warnings, simplefilter

from cartopy import crs, feature as cartopy_feature
from iris import load, analysis, Constraint, plot as iplt
from iris.analysis.stats import pearsonr
from iris.cube import CubeList
from matplotlib import pyplot as plt, use
from matplotlib.colors import CenteredNorm
from numpy import arange

# Iris/conda seem to want to use QT for matplotlib.
use('TKAgg')

# PARAMETERS
# Input command line argument date format
COMMANDLINE_DATE_FORMAT = "%Y%m%d"

# AWAP
AWAP_DIR = "/g/data/zv2/agcd/v1"

TMAX_DIR = join(AWAP_DIR, "{obs_name}", "{obs_aggregate}", "r005", "01day")
TMAX_FILENAME = "agcd_v1_{obs_name}_{obs_aggregate}_r005_daily_{year}.nc"

OBS_NAMES = ["precip", "tmax", "tmin"]
OBS_AGGREGATE_FUNCS = {
    "precip": "total",
    "tmax": "mean",
    "tmin": "mean"
}

BARRA2_DIR = "/g/data/hd50/barra2/data/prod/{user}/cg406_{year}.r1/{year}/" \
             "{month:02d}/{year}{month:02d}{day:02d}T{hour:02d}00Z/nc"

DT_FORMAT = "%Y%m%dT%H%MZ"

# SLV - single level variables - 2D field of outputs
BARRA2_FORECAST_FILENAME = "SLV1H"

# BARRA measurement names
# scrn = screen temperature = 1-2m off the ground
BARRA2_MAX_TEMP_MEASUREMENT = "max_temp_scrn"
BARRA2_MIN_TEMP_MEASUREMENT = "min_temp_scrn"
BARRA2_PRECIP_MEASUREMENT = "av_prcp_rate"

# BARRA2 Central Longitude for Cartopy
BARRA2_CENTRAL_LON = 150

# Figure parameters
colourmap_name = "turbo"
diff_colourmap_name = "RdBu_r"

# Output parameters
OUT_FORMAT = ".png"
OUT_FILENAME_TEMPLATE = "{year:04d}{month:02d}{day:02d}-{obs_name}" + OUT_FORMAT
OUT_DIR = "/home/548/jt4085/testing"


# METHODS
# Data methods
def get_barra2_cycle_data(dt, obs_name, measurement, temp_dir):
    # Use the datetime to find the appropriate directory/suite/cycle
    path = BARRA2_DIR.format(
        user="*", year=dt.year, month=dt.month, day=dt.day, hour=dt.hour)

    path = join(path, BARRA2_FORECAST_FILENAME + ".tar")

    try:
        archive_filepath = glob(path)[0]
    except IndexError:
        # globs returns an empty list when there's no matching files and we'll
        # have an IndexError
        msg = "No matching BARRA2 files for {} on {} found at {}".format(
            obs_name, dt, path)
        print(msg)

        raise FileNotFoundError(msg)

    # Unpack tars to temporary location
    cycle_temp_dir = join(temp_dir, obs_name + '-' + dt.strftime(DT_FORMAT))
    mkdir(cycle_temp_dir)
    unpack_archive(archive_filepath, cycle_temp_dir)

    # File in the archive are something like:
    # -nc/PRS1H
    #  -air_temp_uv-barra_r2-hres-201707020300-201707020900.nc
    #  -frac_time_p_above-barra_r2-hres-201707020300-201707020900.nc
    #  ...
    filepath = glob(join(cycle_temp_dir,
                         "nc",
                         BARRA2_FORECAST_FILENAME,
                         measurement + "*.nc"))[0]

    # Load the cube
    cube = get_data_iris(filepath)

    return cube


def get_data_for_day(target_date, obs_name, temporary_dir, regrid=True):
    # AWAP
    cube_awap = get_awap_data_for_day(target_date, obs_name)

    # BARRA2
    cube_barra = get_barra2_data_for_date(
        target_date, temporary_dir, obs_name)

    # Convert BARRA2 units to match AWAP units
    if obs_name == "tmax" or obs_name == "tmin":
        # Convert barra from K to degrees C to match AWAP
        cube_barra.convert_units("celsius")
    elif obs_name == "precip":
        # BARRA2 uses kg per m^2 per s
        # 1 kg per m^2 of water = 1mm of thickness
        # We've already summed the flux for each hour so we only
        # need to convert from per second to per hour
        cube_barra.convert_units('kg m-2 hour-1')
        cube_barra.rename("thickness_of_precipitation")
        cube_barra.units = "mm"

        # Rename AWAP to have shorter matching name
        cube_awap.rename("thickness_of_precipitation")

    # Regrid barra to match the smaller awap grid
    if regrid:
        # Remove the coord system from awap to allow regridding
        # Should I instead be adding a coord system to barra?
        cube_awap.coord("latitude").coord_system = None
        cube_awap.coord("longitude").coord_system = None

        cube_barra = cube_barra.regrid(cube_awap, analysis.Linear())

    return cube_awap, cube_barra


def get_awap_data_for_day(target_date, obs_name):
    if obs_name not in OBS_NAMES:
        raise ValueError(
            "obs_name ({}) not valid, should be one of {}.".format(
                obs_name, ", ".join(OBS_NAMES)))

    print("\tGetting AWAP data for:", target_date)

    # "Maximum and minimum temperatures for the previous 24 hours are nominally
    # recorded at 9 am local clock time. Minimum temperature is recorded against
    # the day of observation, and the maximum temperature against the previous
    # day."
    # http://www.bom.gov.au/climate/cdo/about/definitionstemp.shtml
    if obs_name == "tmin":
        target_date += timedelta(days=1)
    elif obs_name == "tmax":
        pass
    elif obs_name == "precip":
        target_date += timedelta(days=1)

    # Set the obs_name and year in the path
    path = join(TMAX_DIR, TMAX_FILENAME).format(
        obs_name=obs_name,
        obs_aggregate=OBS_AGGREGATE_FUNCS[obs_name],
        year=target_date.year)

    # Get the filename
    try:
        filepath = glob(path)[0]
    except IndexError:
        # globs returns an empty list when there's no matching files, so we'll
        # have an IndexError
        msg = "No matching AWAP files for {} on {} found for AWAP at {}".format(
            obs_name, target_date, path)
        print(msg)

        raise FileNotFoundError(msg)

    # Load the data file
    cube = get_awap_data(filepath)

    # Note on timezones
    #  AWAP data is in the "local timezone"
    #  Presumably that is with daylight savings time?
    #  As AWAP is daily data, we can't adjust to UTC here,
    #  BARRA2's data is in utc and in hourly bins thus it can be adjusted.

    # Filter out times earlier than datetime
    dt = datetime.combine(target_date, time())
    cube_slice = cube.extract(
        Constraint(time=lambda cell: cell.point > dt)
    )

    # Take the first time after datetime
    cube_slice = cube_slice[0, :, :]

    return cube_slice


def get_awap_data(filepath):
    cube = get_data_iris(filepath)

    return cube


def get_data_iris(filepath):
    cube = load(filepath)[0]

    return cube


def get_barra2_data_for_date(target_date, temp_dir, obs_name):
    # AWAP 'days' are 9am to 9am in AU local time
    # http://www.bom.gov.au/climate/austmaps/about-temp-maps.shtml

    # Australian timezones ranges from
    #  +8 (Australian Western Standard Time, AWST, Perth
    #  +11 (Australian Eastern Daylight Savings Time, AEDT, Melbourne)
    # Can approximate a 'local time' timezone as +9
    # TODO: handle timezones properly, split barra data into timezone sections
    #  and then extract 9am to 9am for each.

    # BARRA2 forecasts are 3 hours ahead of the bins
    # With the timezone approximation of +9 we thus want 0Z to 0Z
    # So to get 9am to 9am from BARRA2 we need the 18, 00, 6, 12, 18 cycles
    cycle_end = datetime.combine(target_date, time(hour=18))
    cycle_start = cycle_end - timedelta(days=1)

    cycle_dts = arange(cycle_start, cycle_end + timedelta(seconds=1),
                       timedelta(hours=6)).astype(datetime)

    # Set the measurement name and aggregate func for each obs
    if obs_name == "tmax":
        measurement = BARRA2_MAX_TEMP_MEASUREMENT
        aggregate_func = analysis.MAX
    elif obs_name == "tmin":
        measurement = BARRA2_MIN_TEMP_MEASUREMENT
        aggregate_func = analysis.MIN
    elif obs_name == "precip":
        measurement = BARRA2_PRECIP_MEASUREMENT
        aggregate_func = analysis.SUM
    else:
        raise ValueError(
            "obs_name ({}) not valid, should be one of {}.".format(
                obs_name, ", ".join(OBS_NAMES)))

    cubes = CubeList([])
    for dt in cycle_dts:
        print("\tGetting BARRA2 data for:",  dt)

        cube = get_barra2_cycle_data(dt, obs_name, measurement, temp_dir)

        # Delete all attributes since they aren't used and
        #  interfere with merging.
        cube.attributes = {}

        # Similarly, remove the "forecast_reference_time" scalar coord
        cube.remove_coord("forecast_reference_time")

        cubes.append(cube)

    # Merge the cubes with concatenate
    concat_cube = cubes.concatenate_cube()

    # Trim the ends of the cube since we only need 00Z to 00Z
    earliest = datetime.combine(target_date, time(hour=0))
    latest = earliest + timedelta(days=1)
    time_constraint = Constraint(
        time=lambda cell: earliest <= cell.point <= latest)
    concat_cube = concat_cube.extract(time_constraint)

    # Collapse along the time axis
    # There's a warning here about non-contiguous coordinates
    with catch_warnings():
        simplefilter("ignore")

        concat_cube = concat_cube.collapsed('time', aggregate_func)

    return concat_cube


# Plotting Methods
def plot_contour_map_iris(iris_cube, ax, print_stats=True,
                          cmap=colourmap_name, centered_cmap=False,
                          mask_oceans=False,
                          vmin=None, vmax=None, levels=None,
                          show_rmse=False):

    if mask_oceans:
        # Note: color bars for iris_cube will still reflect the full dataset
        # TODO: add true masking instead of just cosmetic masking
        ax.add_feature(cartopy_feature.OCEAN, zorder=2,
                       edgecolor='black', facecolor='white')
    else:
        ax.coastlines()

    cs = iplt.contourf(iris_cube, levels=levels if levels is not None else 100,
                       antialiased=False,
                       cmap=cmap,
                       norm=CenteredNorm() if centered_cmap else None,
                       vmin=vmin, vmax=vmax)

    bar = plt.colorbar(orientation="horizontal", pad=0.025)
    bar.set_label(iris_cube.units)

    plt.title(iris_cube.name())

    if print_stats:
        cube_min = iris_cube.data.min()
        cube_max = iris_cube.data.max()
        cube_mean = iris_cube.data.mean()
        cube_std = iris_cube.data.std()
        cube_units = iris_cube.units

        title_str = plt.gca().get_title() + \
            "\nmean: {:.2f}, std: {:.2f}".format(cube_mean, cube_std)

        if show_rmse:
            # Calculate the RMS error
            # There's a warning here about non-contiguous coordinates
            with catch_warnings():
                simplefilter("ignore")
                cube_rmse = iris_cube.collapsed(["latitude", "longitude"],
                                                analysis.RMS).data

            title_str += ", rmse: {:.2f}".format(cube_rmse)

        # Add the units to the end
        title_str += " " + str(cube_units)

        plt.title(title_str)

    return cs.levels


def plot_data(obs_name, cube_awap, cube_barra, cube_diff):
    # Plot on a 1x3 grid (default figsize is 8x6)
    nrows, ncols = 1, 3
    plt.figure(figsize=(ncols*4.0, 4.5))

    # Add a figure title, default ypos is 0.98
    plt.suptitle(obs_name, fontweight='bold')

    # Plot AWAP and BARRA2 with shared vmin/vmax
    vmin = min(cube_awap.data.min(), cube_barra.data.min())
    vmax = max(cube_awap.data.max(), cube_barra.data.max())

    # Mask oceans in AWAP since there aren't obs taken at sea
    axis = plt.subplot(nrows, ncols, 1,
                       projection=crs.PlateCarree(central_longitude=BARRA2_CENTRAL_LON))
    levels = plot_contour_map_iris(cube_awap, axis,
                                   mask_oceans=True,
                                   vmin=vmin, vmax=vmax)
    annotation_location = (0.025, 0.025)
    plt.annotate("AWAP", annotation_location, xycoords="axes fraction")

    axis = plt.subplot(nrows, ncols, 2,
                       projection=crs.PlateCarree(central_longitude=BARRA2_CENTRAL_LON))
    plot_contour_map_iris(cube_barra, axis,
                          vmin=vmin, vmax=vmax, levels=levels)
    plt.annotate("BARRA2", annotation_location, xycoords="axes fraction")

    # Plot the diff
    axis = plt.subplot(nrows, ncols, 3,
                       projection=crs.PlateCarree(central_longitude=BARRA2_CENTRAL_LON))
    plot_contour_map_iris(cube_diff, axis,
                          cmap=diff_colourmap_name, centered_cmap=True,
                          mask_oceans=True, show_rmse=True)
    plt.annotate("BARRA2 - AWAP", annotation_location,
                 xycoords="axes fraction")

    plt.tight_layout()


# Top level method
def get_and_plot_data(target_date, output_dir,
                      output_filename_template=OUT_FILENAME_TEMPLATE):
    print("Getting AWAP and BARRA2 data for", target_date)

    # Use a temp_dir to unpack barra archives into.
    # with at this scope so that IRIS' lazy loading doesn't lose the file
    with TemporaryDirectory() as temp_dir:
        for obs_name in OBS_NAMES:
            print("Processing", obs_name)

            # Get the data for AWAP and BARRA2
            cube_awap, cube_barra = get_data_for_day(target_date, obs_name,
                                                     temp_dir)

            # Calculate the difference between the two cubes
            cube_diff = cube_barra - cube_awap
            cube_diff.rename(obs_name + " error (BARRA2 - AWAP)")

            # Calculate Pearson's r spatial correlation coefficient
            # https://en.wikipedia.org/wiki/Pearson_correlation_coefficient
            if False:
                pearsonr_correlation = pearsonr(
                    cube_barra, cube_awap).data

                print(pearsonr_correlation)

            # Plot the data
            print("\tPlotting data")
            plot_data(obs_name, cube_awap, cube_barra, cube_diff)

            # Save the figure
            out_filename = output_filename_template.format(
                year=target_date.year,
                month=target_date.month,
                day=target_date.day,
                obs_name=obs_name)

            out_path = join(output_dir, out_filename)

            print("\tSaving figure to", out_path)
            plt.savefig(out_path)

            print()


# MAIN
def parse_args():
    # Date time func to parse date command line arg
    def valid_date(s):
        try:
            # Parse date from the input string
            return datetime.strptime(s, COMMANDLINE_DATE_FORMAT).date()
        except ValueError:
            raise ArgumentTypeError("Not a valid date: {0!r}".format(s))

    parser = ArgumentParser(prog="awap_diag.py",
                            description="This script gets AWAP data from project zv2 and BARRA2 data from hd50 for a "
                                        "given date and plots them against each other, outputting a figure to the "
                                        "supplied directory as a png image.")

    parser.add_argument("-o", "--output-dir", nargs="?", required=True,
                        help="Output directory for the figures.")
    parser.add_argument("-d", "--date", nargs="?", required=True, type=valid_date,
                        help="Date to grab the data for. Use the format YYYYMMDD.")

    return parser.parse_args()


def main():
    args = parse_args()
    target_date = args.date
    output_dir = args.output_dir

    get_and_plot_data(target_date, output_dir)


if __name__ == "__main__":
    main()
