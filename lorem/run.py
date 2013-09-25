"""
Generate experimental data using the base.txt as a starting point

Creates directories of files that represent changes over time of the base file
"""
import os
import random
import shutil
import zipfile
import ConfigParser
import marshal
import types


class Config:

    def __init__(self, path):
        self.path = path
        self.parser = ConfigParser.RawConfigParser()
        self.parser.read(path)

    def get(self, section, key, default=None):
        try:
            return self.parser.get(section, key)
        except ConfigParser.NoOptionError:
            return default

    def getint(self, section, key, default=None):
        try:
            return self.parser.getint(section, key)
        except ConfigParser.NoOptionError:
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

    def __init__(self, name, base_text, step_function=None, step_count=100):
        self.name = name
        self.data_dir = os.path.join(self.name, 'data')
        self.base_text = base_text
        self.step_count = step_count

        self.summary = Config(os.path.join(self.name, 'summary.txt'))

        if step_function is None:
            serialised = self.summary.get('overall', 'step_function').decode('hex')
            code = marshal.loads(serialised)
            step_function = types.FunctionType(code, globals())
        else:
            self.summary.set('overall', 'step_function', marshal.dumps(step_function.func_code).encode('hex'))
        self.step_function = step_function

    def run(self):
        print 'Running experiment', self.name
        for trial in xrange(1, 2):
            print '    performing trial %d' % (trial, )
            self.clean()
            self.summary.set('overall', 'trial_count', trial)
            self.generate()
            self.test()
            self.archive()

    def clean(self):
        print '        cleaning up old data'
        try:
            shutil.rmtree(self.data_dir)
        except OSError:
            pass
        os.makedirs(self.data_dir)

    def generate(self):
        print '        generating new data'
        changed_text = self.base_text
        for counter in xrange(0, self.step_count):
            changed_text = self.step_function(changed_text)
            with open(os.path.join(self.data_dir, '%d.txt' % (counter, )), "w") as f:
                f.write(changed_text)

    def test(self):
        print '        running compression tests'
        self.__raw_size()
        self.__zipfiles()
        self.summary.save()

    def archive(self):
        print '        archiving data'
        prefix = 'archive-%s-' % (self.name, )
        count = len([name for name in os.listdir('archive') if name.startswith(prefix)])
        with zipfile.ZipFile(os.path.join('archive', '%s%d.zip' % (prefix, count + 1, )), 'w') as archive:
            for (root, dirs, files) in os.walk(self.name):
                archive.write(os.path.join(root))
                for f in files:
                    archive.write(os.path.join(root, f))

    def __raw_size(self):
        print '            calculating raw size'
        size = 0
        for (root, dirs, files) in os.walk(self.data_dir):
            for f in files:
                size += os.path.getsize(os.path.join(root, f))
        average = self.__update_average('raw_size', size)
        print '                  trial: %d' % (size, )
        print '                average: %d' % (average, )

    def __zipfiles(self):
        print '            zipping files together'
        path = os.path.join(self.name, 'zippedfiles.zip')
        with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as archive:
            for (root, dirs, files) in os.walk(self.data_dir):
                for f in files:
                    archive.write(os.path.join(root, f), f)
        size = os.path.getsize(path)
        average = self.__update_average('zipped_files_size', size)
        print '                  trial: %d' % (size, )
        print '                average: %d' % (average, )

    def __update_average(self, name, value):
        average = self.summary.getint('overall', name, 0)
        trial_count = self.summary.getint('overall', 'trial_count')
        average = (average * (trial_count - 1) + value) / trial_count
        self.summary.set('overall', name, average)
        return average


def run():
    with open('base.txt', 'r') as fd:
        base_text = fd.read()
    experiments = [
        Experiment('single_char_delete', base_text, lambda t: Experiment.delete(t, 1), len(base_text)),
        Experiment('ten_char_delete', base_text, lambda t: Experiment.delete(t, 10), len(base_text) / 10),
        Experiment('hundred_char_delete', base_text, lambda t: Experiment.delete(t, 100), len(base_text) / 100),
        Experiment('thousand_char_delete', base_text, lambda t: Experiment.delete(t, 1000), len(base_text) / 1000),
        Experiment('single_char_insert', base_text, lambda t: Experiment.insert(t, 1), len(base_text)),
        Experiment('ten_char_insert', base_text, lambda t: Experiment.insert(t, 10), len(base_text) / 10),
        Experiment('hundred_char_insert', base_text, lambda t: Experiment.insert(t, 100), len(base_text) / 100),
        Experiment('thousand_char_insert', base_text, lambda t: Experiment.insert(t, 1000), len(base_text) / 1000)
    ]
    for exp in experiments:
        exp.run()

if __name__ == "__main__":
    run()
