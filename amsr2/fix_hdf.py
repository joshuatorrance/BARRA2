
from sys import argv
from h5py import File, string_dtype

filepath = argv[1]

with File(filepath, 'r+') as hdf:
    start_str = hdf.attrs["ObservationStartDateTime"][0]

    hdf.attrs.create("ObservationStartDateTime", [start_str], dtype=string_dtype(encoding='ascii'))


