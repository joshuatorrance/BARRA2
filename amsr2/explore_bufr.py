#!/bin/python3

from glob import glob
from pybufrkit.decoder import Decoder

# Parameters
data_dir = "data"
file_extension = ".bufr"


# Script
decoder = Decoder()

bufr_files = glob(data_dir + "/*" + file_extension)
for bufr_file in bufr_files:
    print(bufr_file)

    with open(bufr_file, 'rb') as bufr_in:
        bufr_message = decoder.process(bufr_in.read())


