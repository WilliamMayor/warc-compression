"""
Generate experimental data using the base.txt as a starting point

Creates directories of files that represent changes over time of the base file
"""
import os
import random
import shutil
import zipfile
import ConfigParser
import tarfile

from operator import itemgetter

from tabulate import tabulate


class Config:

    def __init__(self, path):
        self.path = path
        self.parser = ConfigParser.RawConfigParser()
        self.parser.read(path)

    def get(self, section, key, default=None):
        try:
            return self.parser.get(section, key)
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            return default

    def getint(self, section, key, default=None):
        try:
            return self.parser.getint(section, key)
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            return default

    def set(self, section, key, value):
        try:
            self.parser.set(section, key, value)
        except ConfigParser.NoSectionError:
            self.parser.add_section(section)
            self.parser.set(section, key, value)

    def save(self):
        with open(self.path, 'wb') as fd:
            self.parser.write(fd)


class Experiment:

    @staticmethod
    def identity(text):
        return text

    @staticmethod
    def delete(text, blocksize=1):
        position = random.randint(0, len(text) - blocksize)
        return text[:position] + text[(position+blocksize):]

    @staticmethod
    def insert(text, blocksize=1):
        new_chars = [random.choice(text) for _ in xrange(blocksize)]
        position = random.randint(0, len(text))
        return text[:position] + ''.join(new_chars) + text[position:]

    def __init__(self, name, factory):
        self.name = name
        self.factory = factory

        self.data_dir = os.path.join(self.name, 'data')
        self.summary = Config(os.path.join(self.name, 'summary.txt'))

        self.encodings = {
            'raw': self.encode_raw,
            'zip': self.encode_zip,
            'tar': self.encode_tar,
            'tar_gz': self.encode_tar_gz,
            'tar_bz2': self.encode_tar_bz2
            # use diff algorithms (diff, diffe, vcdiff, etc.)
            #   diff from previous file
            #   diff from first files
            #   diff from preceeding file (iframe)
        }

    def run(self):
        print 'Running experiment', self.name
        start = self.summary.getint('overall', 'trial_count', 1) + 1
        end = start + 1
        for trial in xrange(start, end):
            print '    performing trial %d' % (trial, )
            self.clean()
            self.summary.set('overall', 'trial_count', trial)
            self.generate()
            self.test()
            self.archive()
        self.print_summary()

    def clean(self):
        print '        cleaning up old data'
        try:
            shutil.rmtree(self.data_dir)
        except OSError:
            pass
        os.makedirs(self.data_dir)

    def generate(self):
        print '        generating new data'
        counter = 0
        g = self.factory()
        for text in g:
            data_path = os.path.join(self.data_dir, '%d.txt' % (counter, ))
            with open(data_path, "w") as f:
                f.write(text)
            counter += 1

    def test(self):
        print '        running compression tests'
        for encoding in self.encodings.values():
            encoding()
        self.summary.save()

    def archive(self):
        print '        archiving data'
        try:
            os.makedirs('archive')
        except OSError:
            pass
        prefix = 'archive-%s-' % (self.name, )
        current = [f for f in os.listdir('archive') if f.startswith(prefix)]
        count = len(current)
        path = os.path.join('archive', '%s%d.zip' % (prefix, count + 1, ))
        with zipfile.ZipFile(path, 'w') as archive:
            for (root, dirs, files) in os.walk(self.name):
                archive.write(os.path.join(root))
                for f in files:
                    archive.write(os.path.join(root, f))

    def print_summary(self):
        print 'Summary of', self.name
        data = []
        base_line = self.summary.getint('raw', 'size_mean')
        for name in self.encodings:
            mean = self.summary.get(name, 'size_mean')
            variance = self.summary.get(name, 'size_variance')
            data.append([name, mean, variance])
        data = sorted(data, key=itemgetter(1))
        for row in data:
            row[1] = '%s (%d%%)' % (row[1], 100 * int(row[1]) / base_line)
        headers = ['encoding', 'mean size (%)', 'size variance']
        print tabulate(data, headers)

    def encode_raw(self):
        print '            calculating raw size'
        size = 0
        for (root, dirs, files) in os.walk(self.data_dir):
            for f in files:
                size += os.path.getsize(os.path.join(root, f))
        self.update_averages('raw', size)

    def encode_zip(self):
        print '            zipping files together'
        path = os.path.join(self.name, 'data.zip')
        with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as archive:
            for (root, dirs, files) in os.walk(self.data_dir):
                for f in files:
                    archive.write(os.path.join(root, f), f)
        size = os.path.getsize(path)
        self.update_averages('zip', size)

    def encode_tar(self):
        print '            adding files to tar'
        path = os.path.join(self.name, 'data.tar')
        with tarfile.open(path, 'w') as archive:
            for (root, dirs, files) in os.walk(self.data_dir):
                for f in files:
                    archive.add(os.path.join(root, f), f)
        size = os.path.getsize(path)
        self.update_averages('tar', size)

    def encode_tar_gz(self):
        print '            adding files to tar.gz'
        path = os.path.join(self.name, 'data.tar.gz')
        with tarfile.open(path, 'w:gz') as archive:
            for (root, dirs, files) in os.walk(self.data_dir):
                for f in files:
                    archive.add(os.path.join(root, f), f)
        size = os.path.getsize(path)
        self.update_averages('tar_gz', size)

    def encode_tar_bz2(self):
        print '            adding files to tar.bz2'
        path = os.path.join(self.name, 'data.tar.bz2')
        with tarfile.open(path, 'w:bz2') as archive:
            for (root, dirs, files) in os.walk(self.data_dir):
                for f in files:
                    archive.add(os.path.join(root, f), f)
        size = os.path.getsize(path)
        self.update_averages('tar_bz2', size)

    def update_averages(self, name, size):
        mean = self.summary.getint(name, 'size_mean', 0)
        trial_count = self.summary.getint('overall', 'trial_count', 1)
        m2 = self.summary.getint(name, 'size_m2', 0)

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


def data_generator_factory(text, func, limit=float('inf')):
    def g():
        yield text
        count = 0
        previous = text
        while count < limit:
            count += 1
            current = func(previous)
            if current == previous:
                break
            yield current
            previous = current
    return g
