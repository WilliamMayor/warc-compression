import shutil
import sys
import string
import sqlite3
import time
import random
import os

from collections import defaultdict

import sh

__DATA_DIR = '/scratch0/wmayor/'
__HOME_DIR = '/home/wmayor/generated/'

__DATA_DIR = '/Users/william/Desktop/generated/data/'
__HOME_DIR = '/Users/william/Desktop/generated/home/'

__VCDIFF_PATH = '/usr/local/bin/'
__ZPAQ_PATH = '/Users/william/bin/zpaq649/'
os.environ['PATH'] = os.environ['PATH'] + ':%s:%s' % (__VCDIFF_PATH, __ZPAQ_PATH)

__NUM_TRIALS = 10
__NUM_STEPS = 1024
__DATA_SIZE = 1024

sql_schema = """
    CREATE TABLE IF NOT EXISTS delta(
        mutate FLOAT,
        trial NUMBER,
        step NUMBER,
        name TEXT,
        size NUMBER,
        encode_time NUMBER,
        decode_time NUMBER
    );
    CREATE TABLE IF NOT EXISTS compression(
        mutate FLOAT,
        trial NUMBER,
        step NUMBER,
        delta TEXT,
        name TEXT,
        size NUMBER,
        encode_time NUMBER,
        decode_time NUMBER
    );
    CREATE INDEX IF NOT EXISTS idx_delta_mutate ON delta(mutate);
"""
sql_delta_insert = """
    INSERT INTO delta(
        mutate,
        trial,
        step,
        name,
        size,
        encode_time,
        decode_time)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
sql_compression_insert = """
    INSERT INTO compression(
        mutate,
        trial,
        step,
        delta,
        name,
        size,
        encode_time,
        decode_time)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
sql_last_trial = """
    SELECT MAX(trial) FROM delta WHERE mutate = ?
"""


class Experiment(object):

    def __init__(self, db_path, data_dir, mutate_pct):
        self.db_path = db_path
        self.data_dir = data_dir
        self.mutate_pct = mutate_pct
        self.trial_count = 10
        self.step_count = 1024
        self.data_size = 1024
        self.deltas = [
            (self.no_delta, self.un_no_delta),
            (self.diff, self.un_diff),
            (self.vcdiff, self.un_vcdiff)]
        self.compressions = compressions = [
            (self.no_compression, self.no_compression),
            (self.gzip, self.un_gzip),
            (self.bzip2, self.un_bzip2),
            (self.zpaq, self.un_zpaq)]

    def new_path(self, suffix=''):
        name = ''.join(random.sample(string.lowercase, 5)) + suffix
        path = os.path.join(self.data_dir, name)
        if os.path.exists(path):
            return new_path(suffix=suffix)
        return path

    def write(self, contents, fd=None, suffix=''):
        if fd is None:
            path = self.new_path(suffix=suffix)
            fd = open(path, 'a+b')
        fd.write(contents)
        fd.flush()
        os.fsync(fd)
        return fd

    def compressor(self, do, undo, content):
        tick = [time.time()]
        compressed = do(content)
        tick.append(time.time())
        undo(compressed)
        tick.append(time.time())
        return do.__name__, len(compressed), tick[1] - tick[0], tick[2] - tick[1]

    def differencer(self, do, undo, dictionary, target):
        tick = [time.time()]
        delta = do(dictionary, target)
        tick.append(time.time())
        undo(dictionary, delta)
        tick.append(time.time())
        return do.__name__, delta, tick[1] - tick[0], tick[2] - tick[1]

    def mutate(self, current, count):
        n = len(string.printable) - 1
        indicies = random.sample(xrange(len(current)), count)
        for j, k in enumerate(indicies):
            c = string.printable[random.randint(0, n)]
            while c == current[k]:
                c = string.printable[random.randint(0, n)]
            current[k] = c
        return current

    def trial(self):
        mutation_count = int(self.mutate_pct * self.data_size)
        previous = ''
        current = self.mutate(list(xrange(self.data_size)), self.data_size)
        patches = defaultdict(lambda: '')
        inserts = defaultdict(list)
        for i in xrange(0, self.step_count):
            fd_previous = self.write(previous)
            previous = ''.join(current)
            fd_to = self.write(previous)
            for d, ud in self.deltas:
                dname, patch, dtt, udtt = self.differencer(d, ud, fd_previous, fd_to)
                args = (i, dname, len(patch or previous), dtt, udtt)
                inserts[sql_delta_insert].append(args)
                patches[d] = patches[d] + (patch or previous)
                for c, uc in self.compressions:
                    cname, csize, ctt, uctt = self.compressor(c, uc, patches[d])
                    args = (i, dname, cname, csize, ctt, uctt)
                    inserts[sql_compression_insert].append(args)
            current = self.mutate(current, mutation_count)
            os.remove(fd_previous.name)
            os.remove(fd_to.name)
        return inserts

    def run(self):
        conn = sqlite3.connect(self.db_path, timeout=300.0)
        cursor = conn.cursor()
        cursor.executescript(sql_schema)
        conn.commit()
        cursor.execute(sql_last_trial, (self.mutate_pct, ))
        try:
            t = cursor.fetchone()[0] + 1
        except:
            t = 0
        total, count = 0, 0

        for i in xrange(t, t + self.trial_count):
            tick = time.time()
            inserts = self.trial()
            for sql, values in inserts.iteritems():
                cursor.executemany(sql, [(self.mutate_pct, i) + args for args in values])
            conn.commit()
            tt = time.time() - tick
            total += tt
            count += 1
            print '\nTrial took %dsecs (mean %dsecs)' % (tt, (total / count))
        conn.close()

    def zpaq(self, content):
        fd_in = self.write(content)
        path = self.new_path(suffix='.zpaq')
        sh.zpaq('add', path, fd_in.name, '-method', '6')
        fd = open(path, 'rb')
        r = fd.read()
        os.remove(fd_in.name)
        os.remove(fd.name)
        return r

    def un_zpaq(self, content):
        fd_in = self.write(content, suffix='.zpaq')
        d = self.new_path()
        sh.zpaq('extract', fd_in.name, '-to', d)
        try:
            for r, dirs, files in os.walk(d):
                for f in files:
                    with open(os.path.join(r, f), 'rb') as fd:
                        return fd.read()
        finally:
            os.remove(fd_in.name)
            shutil.rmtree(d)

    def bzip2(self, content):
        return sh.bzip2('--best', '--stdout', _tty_out=False, _in=content).stdout

    def un_bzip2(self, content):
        return sh.bzcat(_in=content).stdout

    def gzip(self, content):
        return sh.gzip('--best', '--stdout', _tty_out=False, _in=content).stdout

    def un_gzip(self, content):
        return sh.gunzip(_in=content).stdout

    def no_compression(self, content):
        return content

    def no_delta(self, dictionary, target):
        target.seek(0)
        return target.read()

    def un_no_delta(self, dictionary, delta):
        return delta

    def diff(self, dictionary, target):
        try:
            p = sh.diff(
                dictionary.name, target.name,
                text=True, ed=True, minimal=True, _ok_code=[0, 1])
            return p.stdout
        except sh.ErrorReturnCode_2 as e:
            if 'No newline at end of file' in e.stderr:
                return e.stdout
            else:
                raise e

    def un_diff(self, dictionary, patch):
        dictionary.seek(0)
        fd = self.write(dictionary.read())
        sh.ed(fd.name, _in=patch + 'w\n')
        fd.seek(0)
        content = fd.read()
        os.remove(fd.name)
        return content[:-1]

    def vcdiff(self, dictionary, target):
        return sh.vcdiff(
            'delta',
            '-dictionary',
            dictionary.name,
            '-target',
            target.name).stdout

    def un_vcdiff(self, dictionary, patch):
        fd = self.write(patch)
        content = sh.vcdiff(
            'patch',
            '-dictionary', dictionary.name,
            '-delta', fd.name).stdout
        os.remove(fd.name)
        return content


if __name__ == '__main__':
    data_dir = os.path.join(__DATA_DIR, sys.argv[1])
    try:
        os.makedirs(data_dir)
    except:
        pass
    mutate_pct = float(sys.argv[1])
    db_path = os.path.join(__HOME_DIR, 'results.db')
    Experiment(db_path, data_dir, mutate_pct).run()
