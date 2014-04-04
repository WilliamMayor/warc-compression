import pprint
import math
import os
import sys
import sqlite3

from itertools import ifilter

from collections import defaultdict

import pylab as P
import numpy as np
import matplotlib.pyplot as plt

import graph
import run

if __name__ == '__main__':
    DB_PATH = '/Users/william/Desktop/generated.db'
    raw, mutations, deltas, compressions = graph.prep(DB_PATH)

    lines = {}

    for d in deltas:
        for c in compressions:
            data = graph.data(raw, delta=d, compression=c)
            slopes = []
            offsets = []
            for m in mutations:
                size = graph.data(data, mutate=m)[0][3][0]
                x = np.array([run.__DATA_SIZE * i for i in xrange(len(size))])
                p = np.polyfit(x, size, 1)
                slopes.append(p[0])
                offsets.append(p[1])
            p = np.polyfit([0.0, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0], slopes, 3)
            lines['%s-%s' % (d, c)] = p

    P.clf()
    f = P.figure()
    a = f.add_subplot(111)
    x = np.linspace(0, 1, 20)
    for label, p in lines.iteritems():
        P.plot(x, p[0] * x ** 3 + p[1] * x ** 2 + p[2] * x + p[3], label=label)
    handles, labels = a.get_legend_handles_labels()
    a.legend(handles, labels)
    f.savefig('/Users/william/Desktop/test.png')
    P.close(f)