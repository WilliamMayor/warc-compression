"""
Generate experimental data using the base.txt as a starting point

Creates directories of files that represent changes over time of the base file
"""
import os
from operator import itemgetter
from tabulate import tabulate

from warcompress.Config import Config
from warcompress.encodings import raw
from warcompress.encodings.compression import (
    _zip,
    gz,
    bzip2,
    optimal
)
from warcompress.encodings.delta import (
    bsdiff,
    diffe,
    diffe_gz,
    vcdiff
)


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

    def __run_test(self, name, module, *args):
        print '           ', name
        compressed_data_path = module.encode(self.data_path, *args)
        self.update_averages(name, compressed_data_path)

    def test(self):
        print '        running encoding tests'
        iframe_options = [0, 1, 2, 5, 10]
        tests = [
            # (name, module, options)
            ('raw',
             raw,
             None),
            ('bz2',
             bzip2,
             None),
            ('gzip',
             gz,
             None),
            ('zip',
             _zip,
             None),
            ('{o} order optimal',
             optimal,
             xrange(0, 10)),
            ('bsdiff, iframe @ {o}',
             bsdiff,
             iframe_options),
            ('diffe, iframe @ {o}',
             diffe,
             iframe_options),
            ('diffe_gz, iframe @ {o}',
             diffe_gz,
             iframe_options),
            ('vcdiff, iframe @ {o}',
             vcdiff,
             iframe_options)
        ]
        for (name, module, options) in tests:
            if options is None:
                self.__run_test(name, module)
            else:
                for o in options:
                    new_name = name.format(o=o)
                    self.__run_test(new_name, module, o)
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
            row.append((100.0 * row[1]) / raw_mean)
            row.append((100.0 * row[1]) / data[0][1])
        headers = [
            'encoding',
            'mean size',
            'size variance',
            '% of raw',
            '% of best']
        print tabulate(data, headers, floatfmt=".2f")
