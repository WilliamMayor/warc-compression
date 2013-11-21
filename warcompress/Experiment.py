"""
Generate experimental data using the base.txt as a starting point

Creates directories of files that represent changes over time of the base file
"""
import sys
import os

from operator import itemgetter
from tabulate import tabulate

from warcompress.Config import Config
from warcompress.encodings import raw, utilities
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
from warcompress.modifiers.text import ( # NOQA
    TextDelete,
    TextInsert,
    TextSubstitute
)
from warcompress.modifiers.css import ( # NOQA
    CssDelete,
    CssInsertDeclaration,
    CssInsertRule
)
from warcompress.modifiers.image import ( # NOQA
    ImageScale,
    ImageRotate,
    ImageCrop,
    ImageGrayScale,
    ImageModulate
)
from warcompress.modifiers.identity import Identity # NOQA


class Experiment:

    def __init__(self, base_path, modifier, data_dir):
        self.base_path = base_path
        self.data_dir = data_dir
        self.modifier = modifier
        self.summary = Config(os.path.join(data_dir, 'summary.txt'))

    def run(self, end=1):
        self.summary.set('overall', 'base_path', self.base_path)
        self.summary.set('overall', 'modifier_name', self.modifier.name)
        print 'Running experiment'
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
        self.data_path = os.path.join(
            self.data_dir,
            'data%s' % os.path.splitext(self.base_path)[1]
        )
        with open(self.base_path, 'rb') as fd:
            base_data = fd.read()
        with open(self.data_path, 'wb') as fd:
            start = True
            for modified in self.modifier.modify(base_data):
                if not start:
                    fd.write(utilities.RECORD_SEPARATOR)
                else:
                    start = False
                fd.write(modified)

    def __run_test(self, name, module, *args):
        print '           ', name
        compressed_data_path = module.encode(self.data_path, *args)
        self.update_averages(name, compressed_data_path)

    def test(self):
        print '        running encoding tests'
        iframe_options = [0, 1, 2, 5, 10]
        tests = [
            # (name, module, options)
            ('raw', raw, None),
            ('bz2', bzip2, None),
            ('gzip', gz, None),
            ('zip', _zip, None),
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
        print ''
        print 'Summary'
        print '-------'
        print 'Base data:', self.summary.get('overall', 'base_path')
        print 'Modifier:', self.summary.get('overall', 'modifier_name')
        print ''
        print 'Results:'
        data = []
        raw_mean = self.summary.getfloat('raw', 'size_mean')
        for name in filter(lambda n: n != 'overall', self.summary.sections()):
            mean = self.summary.getfloat(name, 'size_mean')
            variance = self.summary.getfloat(name, 'size_variance')
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
        print ''


def run(args):
    base_path = args[0]
    data_dir = args[1]
    try:
        os.makedirs(data_dir)
    except:
        pass
    modifier_name = args[2]
    modifier = globals()[modifier_name](*args[3:])
    e = Experiment(base_path, modifier, data_dir)
    e.run()


def summarise(path):
    e = Experiment(None, None, os.path.dirname(path))
    e.print_summary()


def help():
    lines = [
        'python Experiments.py SUMMARY',
        'python Experiments.py BASE_DATA DEST_DIR MODIFIER [OPTIONS])',
        '',
        'This script runs a compression experiment on data generated from a base file.', # NOQA
        '',
        'SUMMARY - The path to a previously generated experiment summary file, called summary.txt', # NOQA
        'BASE_DATA - The file that should be used as a base to generate experimental data', # NOQA
        'DEST_DIR - The directory that experimental data should be stored in. This directory will be created if it does not already exist', # NOQA
        'MODIFIER - The name of the modifier to use to generate data. See below', # NOQA
        ''
        'e.g. python warcompress/Experiment.py data/lorem.txt data/lorem/substitute/ 10 10', # NOQA
        '',
        'Modifiers:',
        '  Identity [REPEAT]',
        '    Repeat the data, unchanged REPEAT (default 20) times.',
        '  TextDelete [CHARS [REPEAT]]',
        '    Deletes CHARS character(s) (default 1) in a randomly selected block.', # NOQA
        '    Repeat REPEAT (default 20) time(s)',
        '  TextInsert [CHARS [REPEAT]]',
        '    Inserts CHARS character(s) (default 1) in a randomly selected block.', # NOQA
        '    Repeat REPEAT (default 20) time(s)',
        '  TextSubstitute [CHARS [REPEAT]]',
        '    Replaces CHARS character(s) (default 1) in a randomly selected block with other characters.', # NOQA
        '    Repeat REPEAT (default 20) time(s)',
        '  CssDelete [DECLRS [REPEAT]]',
        '    Delete DECLRS (default 1) declaration(s) from the base CSS file.',
        '    Repeat REPEAT (default 20) time(s).',
        '  CssInsertDeclaration [DECLRS [REPEAT]]',
        '    Insert DECLRS (default 1) new declaration(s) to a randomly selected rule.', # NOQA
        '    Repeat REPEAT (default 20) time(s).',
        '  CssInsertRule [RULES [REPEAT]]',
        '    Insert RULES (default 1) randomly created new rule(s).', # NOQA
        '    Repeat REPEAT (default 20) time(s).'
    ]
    print '\n'.join(lines)

if __name__ == '__main__':
    if len(sys.argv) == 1 or sys.argv[1] == 'help':
        help()
    elif len(sys.argv) == 2:
        summarise(sys.argv[1])
    else:
        run(sys.argv[1:])
