from __future__ import print_function
import numpy as np
from matplotlib.mlab import csv2rec
import matplotlib.pyplot as plt
from matplotlib.ticker import Formatter


class IntradayChartFormatter(Formatter):
    def __init__(self, datetimes):
        self.datetimes = datetimes

    def __call__(self, x, pos=0):
        'Return the label for time x at position pos'
        ind = int(np.round(x))
        if ind >= len(self.datetimes) or ind < 0:
            return ''

        return self.datetimes[ind]
