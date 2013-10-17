"""
Generate experimental data using the base.txt as a starting point

Creates directories of files that represent changes over time of the base file
"""
import os
import itertools
from operator import itemgetter
from tabulate import tabulate

from Config import Config
from warcompress.experiments import encodings


class Experiment:

    def __init__(self, name, factory):
        self.name = name
        self.factory = factory
        self.summary = Config(os.path.join(factory.dir, 'summary.txt'))

    def run(self, end=1):
        print 'Running experiment', self.name
        start = self.summary.getint('overall', 'trial_count', 0) + 1
        end = start + end
        for trial in xrange(start, end):
            print '    performing trial %d' % (trial, )
            self.summary.set('overall', 'trial_count', trial)
            self.generate()
            self.test()
        self.print_summary()

    def generate(self):
        print '        generating new data'
        self.data_path = self.factory()

    def __ordinal(self, n):
        if 10 <= n % 100 < 20:
            return str(n) + 'th'
        else:
            return str(n) + {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, "th")

    def test(self):
        print '        running compression tests'
        for name, encode in encodings.compression.iteritems():
            print '           ', name
            compressed_data_path = encode(self.data_path)
            self.update_averages(name, compressed_data_path)
        for n in xrange(0, 10):
            name = '%s order optimal' % self.__ordinal(n)
            print '           ', name
            compressed_data_path = encodings.n_order_optimal(self.data_path, n)
            self.update_averages(name, compressed_data_path)
        names = encodings.diff.keys()
        iframes_every = [0, 2, 5, 10]
        is_waterfall = [True, False]
        for (n, d, w) in itertools.product(names, iframes_every, is_waterfall):
            name = ' '.join(['          ',
                             '%s,' % n,
                             'iframe every %d,' % d,
                             'waterfall' if w else 'from iframe'])
            print name
            diff_data_path = encodings.diff[n](self.data_path, d, w)
            self.update_averages(name, diff_data_path)
        self.summary.save()

    def update_averages(self, name, data_path):
        trial_count = self.summary.getint('overall', 'trial_count', 1)

        size = os.stat(data_path).st_size
        mean = self.summary.getfloat(name, 'size_mean', 0.0)
        m2 = self.summary.getfloat(name, 'size_m2', 0.0)

        delta = size - mean
        mean = mean + delta / trial_count
        m2 = m2 + delta * (size - mean)

        if trial_count == 1:
            variance = 0
        else:
            variance = m2 / (trial_count - 1)

        self.summary.set(name, 'size_mean', mean)
        self.summary.set(name, 'size_m2', m2)
        self.summary.set(name, 'size_variance', variance)

    def print_summary(self):
        print 'Summary of', self.name
        data = []
        raw_mean = self.summary.getint('raw', 'size_mean')
        for name in filter(lambda n: n != 'overall', self.summary.sections()):
            mean = self.summary.get(name, 'size_mean')
            variance = self.summary.get(name, 'size_variance')
            data.append([name, mean, variance])
        data = sorted(data, key=itemgetter(1))
        for row in data:
            row.append(100.0 * float(row[1]) / raw_mean)
        headers = [
            'encoding',
            'mean size',
            'size variance',
            '% of raw']
        print tabulate(data, headers, floatfmt=".2f")
