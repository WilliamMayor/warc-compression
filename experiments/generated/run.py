import shutil
import sys
import string
import sqlite3
import time
import random
import os

from collections import defaultdict

import sh

DATA_DIR = '/scratch0/wmayor/'
HOME_DIR = '/home/wmayor/generated/'
DATA_DIR = '/Users/william/Desktop/generated/data/'
HOME_DIR = '/Users/william/Desktop/generated/home/'

VCDIFF_PATH = '/usr/local/bin/'
ZPAQ_PATH = '/Users/william/Downloads/zpaq649/'
os.environ['PATH'] = os.environ['PATH'] + ':%s:%s' % (VCDIFF_PATH, ZPAQ_PATH)

NUM_TRIALS = 1
NUM_STEPS = 10
DATA_SIZE = 1024

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


def new_path():
    name = ''.join(random.sample(string.lowercase, 5))
    path = os.path.join(DATA_DIR, name)
    if os.path.exists(path):
        return new_path()
    return path


def new_file(suffix=''):
    path = new_path() + suffix
    try:
        fd = open(path, 'r+b')
    except IOError:
        fd = open(path, 'a+b')
    return fd


def write(contents, fd=None, newline=False, suffix=''):
    if fd is None:
        fd = new_file(suffix=suffix)
    if newline and not contents.endswith('\n'):
        contents = contents + '\n'
    fd.write(contents)
    fd.flush()
    os.fsync(fd)
    return fd


def compressor(do, undo, content):
    tick = [time.time()]
    compressed = do(content)
    tick.append(time.time())
    #uncompressed = undo(compressed)
    undo(compressed)
    tick.append(time.time())
    #assert uncompressed == content, (do.__name__, uncompressed, content)
    return do.__name__, len(compressed), tick[1] - tick[0], tick[2] - tick[1]


def zpaq(content):
    fd_in = write(content)
    path = new_path() + '.zpaq'
    sh.zpaq('add', path, fd_in.name, '-method', '6')
    fd = open(path, 'rb')
    return fd.read()


def un_zpaq(content):
    fd_in = write(content, suffix='.zpaq')
    d = new_path()
    sh.zpaq('extract', fd_in.name, '-to', d)
    for r, dirs, files in os.walk(d):
        for f in files:
            with open(os.path.join(r, f), 'rb') as fd:
                return fd.read()


def bzip2(content):
    return sh.bzip2('--best', '--stdout', _tty_out=False, _in=content).stdout


def un_bzip2(content):
    return sh.bzcat(_in=content).stdout


def gzip(content):
    return sh.gzip('--best', '--stdout', _tty_out=False, _in=content).stdout


def un_gzip(content):
    return sh.gunzip(_in=content).stdout


def no_compression(content):
    return content


def differencer(do, undo, dictionary, target):
    tick = [time.time()]
    delta = do(dictionary, target)
    tick.append(time.time())
    #original = undo(dictionary, delta)
    undo(dictionary, delta)
    tick.append(time.time())
    #target.seek(0)
    #content = target.read()
    #assert original == content, do.__name__
    return do.__name__, delta, tick[1] - tick[0], tick[2] - tick[1]


def no_delta(dictionary, target):
    target.seek(0)
    return target.read()


def un_no_delta(dictionary, delta):
    return delta


def diff(dictionary, target):
    return sh.diff(
        dictionary.name, target.name,
        text=True, ed=True, minimal=True, _ok_code=[0, 1]).stdout


def un_diff(dictionary, patch):
    sh.ed(dictionary.name, _in=patch + 'w\n')
    dictionary.seek(0)
    content = dictionary.read()
    return content


def vcdiff(dictionary, target):
    return sh.vcdiff(
        'delta',
        '-dictionary',
        dictionary.name,
        '-target',
        target.name).stdout


def un_vcdiff(dictionary, patch):
    fd = write(patch)
    return sh.vcdiff(
        'patch',
        '-dictionary', dictionary.name,
        '-delta', fd.name).stdout


deltas = [
    (no_delta, un_no_delta),
    (diff, un_diff),
    (vcdiff, un_vcdiff)]
compressions = [
    (no_compression, no_compression),
    (gzip, un_gzip),
    (bzip2, un_bzip2),
    (zpaq, un_zpaq)]


def clean_up():
    shutil.rmtree(DATA_DIR)
    os.mkdir(DATA_DIR)


def mutate(current, count):
    n = len(string.printable) - 1
    indicies = random.sample(xrange(DATA_SIZE), count)
    mutations = [
        string.printable[random.randint(0, n)] for i in xrange(count)]
    for j, k in enumerate(indicies):
        current[k] = mutations[j]
    return current


def trial(mutate_pct):
    mutation_count = int(mutate_pct * DATA_SIZE)
    n = len(string.printable) - 1
    _r = random.randint
    previous = ''
    current = [string.printable[_r(0, n)] for i in xrange(DATA_SIZE)]
    patches = {d[0]: '' for d in deltas}
    for i in xrange(0, NUM_STEPS):
        fd_previous = write(previous, newline=True)
        previous = ''.join(current)
        fd_to = write(previous, newline=True)

        for d, ud in deltas:
            dname, patch, dtt, udtt = differencer(d, ud, fd_previous, fd_to)
            args = (i, dname, len(patch or previous), dtt, udtt)
            yield sql_delta_insert, args
            patches[d] = patches[d] + (patch or previous)
            for c, uc in compressions:
                cname, csize, ctt, uctt = compressor(c, uc, patches[d])
                args = (i, dname, cname, csize, ctt, uctt)
                yield sql_compression_insert, args

        current = mutate(current, mutation_count)
        clean_up()


def main(mutation):
    db_path = os.path.join(HOME_DIR, 'results.db')
    conn = sqlite3.connect(db_path, timeout=300.0)
    cursor = conn.cursor()
    cursor.executescript(sql_schema)
    conn.commit()
    cursor.execute(sql_last_trial, (mutation, ))
    try:
        t = cursor.fetchone()[0] + 1
    except:
        t = 0
    total, count = 0, 0

    for i in xrange(t, t + NUM_TRIALS):
        tick = time.time()
        inserts = defaultdict(list)
        for sql, args in trial(mutation):
            inserts[sql].append((mutation, i) + args)
        for sql, values in inserts.iteritems():
            cursor.executemany(sql, values)
        conn.commit()
        tt = time.time() - tick
        total += tt
        count += 1
        print '\nTrial took %dsecs (mean %dsecs)' % (tt, (total / count))
    conn.close()


if __name__ == '__main__':
    DATA_DIR = os.path.join(DATA_DIR, sys.argv[1])
    try:
        os.makedirs(DATA_DIR)
    except:
        pass
    main(float(sys.argv[1]))
