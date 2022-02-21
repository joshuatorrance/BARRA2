#!/bin/env python

import sys

import eccodes as ecc
import numpy as np
from sonde import *


class barra2:
    'main class for barra2'

    f_txt = f_bfr = 0

#============================================================================
    def __init__(self):
        """
        class init
        """

        self.f_txt = open(sys.argv[1], 'r')
        self.f_bfr = open(sys.argv[3], 'wb')


#============================================================================
if __name__ == '__main__':
    if (len(sys.argv) != 4):
        print('Usage: ', sys.argv[0], ' in.txt in.nc out.bufr')
        sys.exit()

    b = barra2()
    t = sonde_txt()
    nc = sonde_nc()
    nc.read(sys.argv[2])

    idx = 0 # pointer to nc date/time

    x = t.read_txt(b.f_txt)
    while x == 1:
        bfr = sonde_bfr(t)
        idx = bfr.write_temp(b.f_bfr, t, nc, idx)
        x = t.read_txt(b.f_txt)

    b.f_txt.close()
    ecc.codes_release(bfr.b_temp) # template file
    bfr.f_temp.close()
    b.f_bfr.close()
