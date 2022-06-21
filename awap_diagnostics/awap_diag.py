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

# Datetime format for cylc
CYCLE_DT_FORMAT = "%Y%m%dT%H%MZ"

# Observation details
OBS_NAMES = ["precip", "tmax", "tmin"]
OBS_AGGREGATE_FUNCS = {
    "precip": "total",
    "tmax": "mean",
    "tmin": "mean"
}

# AWAP data
AWAP_DIR = "/g/data/zv2/agcd/v1"

AWAP_OBS_DIR = join(AWAP_DIR, "{obs_name}", "{obs_aggregate}", "r005", "01day")
AWAP_OBS_FILENAME = "agcd_v1_{obs_name}_{obs_aggregate}_r005_daily_{year}.nc"

# ERA5 data
# https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation
ERA5_DIR = "/g/data/rt52/"

ERA5_OBS_DIR = join(ERA5_DIR, "era5", "single-levels", "reanalysis",
                    "{era5_obs_name}", "{year:04d}")
ERA5_OBS_FILENAME = "{era5_obs_name}_era5_oper_sfc_{year:04d}{month:02d}*.nc"

# This dict maps the obs names to those used by ERA5
ERA5_OBS_NAMES_MAP = {
    "precip": "mtpr",
    "tmax": "2t",
    "tmin": "2t"
}

# BARRA2 data
BARRA2_DIR = "/g/data/hd50/barra2/data/prod/{user}/cg406_{year}.r1/{year}/" \
             "{month:02d}/{year}{month:02d}{day:02d}T{hour:02d}00Z/nc"

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
OUT_FILENAME_TEMPLATE = "{reference_name}_{year:04d}{month:02d}{day:02d}-{obs_name}" + OUT_FORMAT
OUT_DIR = "/home/548/jt4085/testing"


# METHODS
# Data methods
def get_data_iris(filepath):
    cube = load(filepath)[0]

    return cube


def get_data_for_day(target_date, obs_name, temporary_dir, regrid=True,
                     quiet_exceptions=False):
    # ERA5
    try:
        cube_era5 = get_era5_data_for_date(target_date, obs_name)
    except FileNotFoundError as e:
        # Message has already been printed.
        if not quiet_exceptions:
            raise e
        else:
            cube_era5 = None

    # AWAP
    try:
        cube_awap = get_awap_data_for_day(target_date, obs_name)
    except FileNotFoundError as e:
        # Message has already been printed.
        if not quiet_exceptions:
            raise e
        else:
            cube_awap = None

    # BARRA2
    cube_barra = get_barra2_data_for_date(
        target_date, temporary_dir, obs_name,
        quiet_exceptions=quiet_exceptions)

    # Convert BARRA2 & ERA5 units to match AWAP units
    if obs_name == "tmax" or obs_name == "tmin":
        # Convert BARRA2 & ERA5 from K to degrees C to match AWAP
        cube_barra.convert_units("celsius")

        if cube_era5:
            cube_era5.convert_units("celsius")

            # Rename ERA5 from "2 metre temperature" to match the others
            cube_era5.rename("air_temperature")
    elif obs_name == "precip":
        # BARRA2 uses kg per m^2 per s
        # 1 kg per m^2 of water = 1mm of thickness
        # We've already summed the flux for each hour so we only
        # need to convert from per second to per hour
        cube_barra.convert_units('kg m-2 hour-1')
        cube_barra.rename("thickness_of_precipitation")
        cube_barra.units = "mm"

        if cube_era5:
            # Convert ERA5's precip units just like BARRA2's
            cube_era5.convert_units('kg m-2 hour-1')
            cube_era5.rename("thickness_of_precipitation")
            cube_era5.units = "mm"

        if cube_awap:
            # Rename AWAP to have shorter matching name
            cube_awap.rename("thickness_of_precipitation")

    # Regrid BARRA2 & ERA5 to match the smaller AWAP grid
    # TODO: Can regrid ERA5 to BARRA2 grid instead of ERA5 to AWAP to compare
    #  ERA5 and BARRA2.
    if regrid:
        if cube_awap:
            # Remove the coord system from awap to allow regridding
            cube_awap.coord("latitude").coord_system = None
            cube_awap.coord("longitude").coord_system = None

            cube_barra = cube_barra.regrid(cube_awap, analysis.Linear())

            if cube_era5:
                cube_era5 = cube_era5.regrid(cube_awap, analysis.Linear())
        elif cube_era5:
            # If AWAP is missing regrid ERA5 to BARRA2's grid
            cube_era5 = cube_era5.regrid(cube_barra, analysis.Linear())

    return cube_awap, cube_barra, cube_era5


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
    path = join(AWAP_OBS_DIR, AWAP_OBS_FILENAME).format(
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
    cube = get_data_iris(filepath)

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
    cycle_temp_dir = join(temp_dir, obs_name + '-' +
                          dt.strftime(CYCLE_DT_FORMAT))
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


def get_barra2_data_for_date(target_date, temp_dir, obs_name,
                             quiet_exceptions=False):
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

        try:
            cube = get_barra2_cycle_data(dt, obs_name, measurement, temp_dir)
        except FileNotFoundError as e:
            if quiet_exceptions:
                continue
            else:
                raise e

        # Delete all attributes since they aren't used and
        #  interfere with merging.
        cube.attributes = {}

        # Similarly, remove the "forecast_reference_time" scalar coord
        cube.remove_coord("forecast_reference_time")

        cubes.append(cube)

    # If there's no BARRA2 data then there's something's gone wrong and we
    # should fail loudly regardless of quiet_exceptions
    if len(cubes) == 0:
        msg = "No BARRA2 data found for {obs_name} on {date}.".format(
            obs_name=obs_name, date=target_date)
        print(msg)
        raise FileNotFoundError(msg)

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


def get_era5_data_for_date(target_date, obs_name):
    # Set aggregate func for each obs
    if obs_name == "tmax":
        aggregate_func = analysis.MAX
    elif obs_name == "tmin":
        aggregate_func = analysis.MIN
    elif obs_name == "precip":
        aggregate_func = analysis.SUM
    else:
        raise ValueError(
            "obs_name ({}) not valid, should be one of {}.".format(
                obs_name, ", ".join(OBS_NAMES)))

    print("\tGetting ERA5 data for:", target_date)

    # Set the obs_name (for ERA5) and date in the path
    path = join(ERA5_OBS_DIR, ERA5_OBS_FILENAME).format(
        era5_obs_name=ERA5_OBS_NAMES_MAP[obs_name],
        year=target_date.year,
        month=target_date.month)

    # Get the filename
    try:
        filepath = glob(path)[0]
    except IndexError:
        # globs returns an empty list when there's no matching files, so we'll
        # have an IndexError
        msg = "No matching ERA5 files for {} on {} found for AWAP at {}".format(
            obs_name, target_date, path)
        print(msg)

        raise FileNotFoundError(msg)

    # Load the data file
    cube = get_data_iris(filepath)

    # Filter out times earlier than the supplied date
    start = datetime.combine(target_date, time())
    end = datetime.combine(target_date, time()) + timedelta(days=1)
    cube_slice = cube.extract(
        Constraint(time=lambda cell: start <= cell.point < end)
    )

    # Collapse along the time axis
    # There's a warning here about non-contiguous coordinates
    with catch_warnings():
        simplefilter("ignore")

        cube_slice = cube_slice.collapsed('time', aggregate_func)

    return cube_slice


# Plotting Methods
def plot_contour_map_iris(iris_cube, ax, print_stats=True,
                          cmap=colourmap_name, centered_cmap=False,
                          mask_oceans=False,
                          vmin=None, vmax=None, levels=None,
                          show_rmse=False, show_pearson=None):
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

        if show_pearson:
            # Pearson's r correlation is unitless, so put it after the units
            title_str += "\nr: {:.2f}".format(show_pearson)

        plt.title(title_str)

    if mask_oceans:
        # Note: color bars for iris_cube will still reflect the full dataset
        # TODO: add true masking instead of just cosmetic masking
        ax.add_feature(cartopy_feature.OCEAN, zorder=2,
                       edgecolor='black', facecolor='white')
    else:
        ax.coastlines()

    return cs.levels


def plot_data(obs_name, cube_reference, cube_barra, cube_diff, reference_name,
              mask_reference_oceans=False):
    # Plot on a 1x3 grid (default figsize is 8x6)
    nrows, ncols = 1, 3
    plt.figure(figsize=(ncols*4.0, 4.5))

    # Add a figure title, default ypos is 0.98
    plt.suptitle(obs_name, fontweight='bold')

    # Plot Reference and BARRA2 with shared vmin/vmax and levels
    vmin = min(cube_reference.data.min(), cube_barra.data.min())
    vmax = max(cube_reference.data.max(), cube_barra.data.max())

    # Construct levels manually since, add a bit to arange's top level since it not included
    d_level = (vmax-vmin)/100
    levels = arange(vmin, vmax + d_level/2, d_level)

    # Annotate the plots in the bottom left corner
    annotation_location = (0.025, 0.025)

    # BARRA2
    axis = plt.subplot(nrows, ncols, 2,
                       projection=crs.PlateCarree(central_longitude=BARRA2_CENTRAL_LON))
    plot_contour_map_iris(cube_barra, axis,
                          vmin=vmin, vmax=vmax, levels=levels)
    plt.annotate("BARRA2", annotation_location, xycoords="axes fraction")

    # Reference
    axis = plt.subplot(nrows, ncols, 1,
                       projection=crs.PlateCarree(central_longitude=BARRA2_CENTRAL_LON))
    plot_contour_map_iris(cube_reference, axis,
                          mask_oceans=mask_reference_oceans,
                          vmin=vmin, vmax=vmax, levels=levels)
    plt.annotate(reference_name, annotation_location, xycoords="axes fraction")

    # Plot the diff
    # Calculate Pearson's r spatial correlation coefficient
    # https://en.wikipedia.org/wiki/Pearson_correlation_coefficient
    pearsonr_correlation = pearsonr(cube_barra, cube_reference).data

    axis = plt.subplot(nrows, ncols, 3,
                       projection=crs.PlateCarree(central_longitude=BARRA2_CENTRAL_LON))
    plot_contour_map_iris(cube_diff, axis,
                          cmap=diff_colourmap_name, centered_cmap=True,
                          mask_oceans=mask_reference_oceans,
                          show_rmse=True, show_pearson=pearsonr_correlation)
    plt.annotate("BARRA2 - " + reference_name, annotation_location,
                 xycoords="axes fraction")

    plt.tight_layout()


# Top level method
def get_and_plot_data(target_date, output_dir,
                      output_filename_template=OUT_FILENAME_TEMPLATE,
                      quiet_exceptions=False):
    print("Getting AWAP, ERA5, and BARRA2 data for", target_date)

    # Use a temp_dir to unpack barra archives into.
    # with at this scope so that IRIS' lazy loading doesn't lose the file
    with TemporaryDirectory() as temp_dir:
        for obs_name in OBS_NAMES:
            print("Processing", obs_name)

            # Get the data for AWAP and BARRA2
            cube_awap, cube_barra, cube_era5 = get_data_for_day(
                target_date,
                obs_name,
                temp_dir,
                quiet_exceptions=quiet_exceptions)

            # Add refernce cubes to a list so we can handle missing data
            ref_list = []
            if cube_awap:
                ref_list.append((cube_awap, "AWAP"))
            if cube_era5:
                ref_list.append((cube_era5, "ERA5"))
            if len(ref_list) == 0:
                msg = "No reference data to compare BARRA2 to. Skipping figure creation."
                print(msg)

                if not quiet_exceptions:
                    raise FileNotFoundError(msg)

            for cube_ref, ref_name in ref_list:
                # Calculate the difference between BARRA2 and the other two cubes
                cube_diff = cube_barra - cube_ref
                cube_diff.rename(
                    obs_name + " error (BARRA2 - " + ref_name + ")")

                # Plot the data and save the figures
                print("\tPlotting data for", ref_name)

                # Mask oceans in AWAP since there aren't obs taken at sea
                plot_data(obs_name, cube_ref, cube_barra, cube_diff, ref_name,
                          mask_reference_oceans=ref_name == "AWAP")

                # Save figure
                out_filename = output_filename_template.format(
                    year=target_date.year,
                    month=target_date.month,
                    day=target_date.day,
                    obs_name=obs_name,
                    reference_name=ref_name)

                out_path = join(output_dir, out_filename)

                print("\tSaving", ref_name, "figure to", out_path)
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
    parser.add_argument("-f", "--fail-quietly", required=False, action='store_true',
                        help="Don't fail if no file is present for one of the datasets. " +
                             "If none of the datasets for BARRA2 are found then an exception "
                             "will still be thrown.")

    return parser.parse_args()


def main():
    args = parse_args()
    target_date = args.date
    output_dir = args.output_dir
    quiet_exceptions = args.fail_quietly

    get_and_plot_data(target_date, output_dir,
                      quiet_exceptions=quiet_exceptions)


if __name__ == "__main__":
    main()
