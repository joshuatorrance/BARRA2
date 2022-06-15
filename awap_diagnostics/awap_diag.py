# Initial script to explore how to use the AWAP data to validate BARRA2 data.
#
# Docs for netCDF4 can be found here:
# https://unidata.github.io/netcdf4-python/#creatingopeningclosing-a-netcdf-file
#
# Uses the module:
# conda/analysis3-22.04

# IMPORTS
from datetime import date, datetime, time, timedelta
from glob import glob
from os import mkdir
from os.path import join
from shutil import unpack_archive
from tempfile import TemporaryDirectory
from warnings import catch_warnings, simplefilter

from cartopy import crs, feature as cartopy_feature
from iris import load, analysis, Constraint, quickplot as iplt
from iris.cube import CubeList
from matplotlib import pyplot as plt, use
from matplotlib.colors import CenteredNorm
from numpy import arange

# Iris/conda seem to want to use QT for matplotlib.
# Change it back to TK
use('TKAgg')

# PARAMETERS
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
diff_colourmap_name = "RdBu"


# METHODS
# Data methods
def get_barra2_cycle_data(dt, obs_name, measurement, temp_dir):
    # Use the datetime to find the appropriate directory/suite/cycle
    path = BARRA2_DIR.format(
        user="*", year=dt.year, month=dt.month, day=dt.day, hour=dt.hour)

    path = join(path, BARRA2_FORECAST_FILENAME + ".tar")

    archive_filepath = glob(path)[0]

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
    # TODO: I haven't quite wrapped my brain around this, what's below seems to
    #  work but doesn't seem right. Understand what's going on or exhaustively
    #  verify it's correct.
    if obs_name == "tmin":
        target_date += timedelta(days=1)

    # Set the obs_name and year in the path
    path = join(TMAX_DIR, TMAX_FILENAME).format(
        obs_name=obs_name,
        obs_aggregate=OBS_AGGREGATE_FUNCS[obs_name],
        year=target_date.year)

    # Get the filename
    filepath = glob(path)[0]

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


# noinspection PyTypeChecker
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
    time_constraint = Constraint(time=lambda cell: earliest <= cell.point <= latest)
    concat_cube = concat_cube.extract(time_constraint)

    # Collapse along the time axis
    # There's a warning here about non-contiguous coordinates
    with catch_warnings():
        simplefilter("ignore")

        concat_cube = concat_cube.collapsed('time', aggregate_func)

    return concat_cube


# Plotting Methods
def plot_contour_map_iris(iris_cube, title_str=None, print_stats=True,
                          cmap=colourmap_name, centered_cmap=False,
                          mask_oceans=False):
    plt.figure(title_str)

    ax = plt.axes(projection=crs.PlateCarree(
        central_longitude=BARRA2_CENTRAL_LON))

    if mask_oceans:
        # Note: color bars for iris_cube will still reflect the full dataset
        # TODO: add true masking instead of just cosmetic masking
        ax.add_feature(cartopy_feature.OCEAN, zorder=2,
                       edgecolor='black', facecolor='white')
    else:
        ax.coastlines()

    iplt.contourf(iris_cube, levels=100, cmap=cmap,
                  norm=CenteredNorm() if centered_cmap else None)

    if print_stats:
        cube_min = iris_cube.data.min()
        cube_max = iris_cube.data.max()
        cube_mean = iris_cube.data.mean()
        cube_std = iris_cube.data.std()
        cube_units = iris_cube.units

        plt.title(plt.gca().get_title() +
                  "\n(min: {:.2f}, max: {:.2f}, mean: {:.2f}, std: {:.2f} {})".format(
                      cube_min, cube_max, cube_mean, cube_std, cube_units
                  ))


# MAIN
def main():
    # Use a temp_dir to unpack barra archives into.
    # with at this scope so that IRIS lazy loading doesn't lose the file
    with TemporaryDirectory() as temp_dir:
        test_date = date(year=2017, month=9, day=2)

        for obs_name in OBS_NAMES:
            print(obs_name)

            cube_awap, cube_barra = get_data_for_day(test_date, obs_name, temp_dir)

            if True:
                # Mask oceans in AWAP since there aren't obs taken at sea
                plot_contour_map_iris(cube_awap, obs_name + ": AWAP", mask_oceans=True)
                plot_contour_map_iris(cube_barra, obs_name + ": BARRA")

            # Calculate the difference between the two cubes
            cube_diff = cube_barra - cube_awap
            cube_diff.rename(obs_name + " error (BARRA2 - AWAP)")

            # Plot the diff
            plot_contour_map_iris(cube_diff, obs_name + ": BARRA - AWAP",
                                  cmap=diff_colourmap_name, centered_cmap=True,
                                  mask_oceans=True)

            print()

        plt.show()


if __name__ == "__main__":
    main()
