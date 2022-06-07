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

from cartopy import crs
from iris import load, analysis, Constraint, quickplot as iplt
from iris.cube import CubeList
from matplotlib import pyplot as plt, use
from numpy import meshgrid, arange

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


# METHODS
def get_awap_data_for_day(target_date, obs_name):
    if obs_name not in OBS_NAMES:
        raise ValueError(
            "obs_name ({}) not valid, should be one of {}.".format(
                obs_name, ", ".join(OBS_NAMES)))

    print("\tGetting AWAP data for:", target_date)

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


def get_barra2_data_for_date(target_date, temp_dir, obs_name):
    # AWAP 'days' are 9am to 9am in AU local time
    # Australian timezones ranges from
    #  +8 (Australian Western Standard Time, AWST, Perth
    #  +11 (Australian Eastern Daylight Savings Time, AEDT, Melbourne)
    # Can approximate a 'local time' timezone as +9
    # TODO: handle timezones properly, split barra data into timezone sections
    #  and then extract 9am to 9am for each.

    # BARRA2 forecasts are 3 hours ahead of the bins
    # With the timezone approximation of +9 we thus want 0Z to 0Z
    # So to get 9am to 9am from BARRA2 we need the 18, 00, 6, 12, cycles
    start = datetime.combine(target_date, time(hour=18)) - timedelta(days=1)
    end = start + timedelta(days=1)

    cycle_dts = arange(start, end, timedelta(hours=6)).astype(datetime)

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
        cube = get_barra2_data(filepath)

        # Delete all attributes since they aren't used and
        #  interfere with merging.
        cube.attributes = {}

        # Similarly, remove the "forecast_reference_time" scalar coord
        cube.remove_coord("forecast_reference_time")

        cubes.append(cube)

    # Merge the cubes with concatenate
    concat_cube = cubes.concatenate_cube()

    # Collapse along the time axis
    # There's a warning here about non-contiguous coordinates
    with catch_warnings():
        simplefilter("ignore")

        concat_cube = concat_cube.collapsed('time', aggregate_func)

    return concat_cube


def get_barra2_data(filepath):
    cube = get_data_iris(filepath)

    return cube


def plot_contour_map(lons, lats, vals, title=None):
    mesh_lons, mesh_lats = meshgrid(lons, lats)

    ax = plt.axes(projection=crs.PlateCarree(
        central_longitude=BARRA2_CENTRAL_LON))
    ax.coastlines()

    if title:
        plt.title(title)

    plt.contourf(mesh_lons, mesh_lats, vals, levels=100,
                 transform=crs.PlateCarree(), cmap="turbo")

    plt.tight_layout()
    plt.colorbar(orientation="horizontal", shrink=0.7)


def plot_contour_map_iris(iris_slice, title_str=None):
    plt.figure(title_str)

    ax = plt.axes(projection=crs.PlateCarree(
        central_longitude=BARRA2_CENTRAL_LON))
    ax.coastlines()

    iplt.contourf(iris_slice, levels=100, cmap="turbo")


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


# MAIN
def main():
    # Use a temp_dir to unpack barra archives into.
    # with at this scope so that IRIS lazy loading doesn't lose the file
    with TemporaryDirectory() as temp_dir:
        test_date = date(year=2017, month=7, day=2)

        for obs_name in OBS_NAMES:
            print(obs_name)

            cube_awap, cube_barra = get_data_for_day(test_date, obs_name, temp_dir)

            plot_contour_map_iris(cube_awap, obs_name + ": AWAP")
            plot_contour_map_iris(cube_barra, obs_name + ": BARRA")

            # Calculate the difference between the two cubes
            cube_diff = cube_awap - cube_barra

            plot_contour_map_iris(cube_diff, obs_name + ": AWAP - BARRA")

            print()

            break

        plt.show()


if __name__ == "__main__":
    main()
