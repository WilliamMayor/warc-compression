import pprint
import math
import os
import sys
import sqlite3

from itertools import ifilter

from collections import defaultdict

import pylab as P
import numpy as np

import run

sql_index = """
    CREATE INDEX IF NOT EXISTS idx_size_for_step_compression
        ON compression(delta, name, mutate, step);
    CREATE INDEX IF NOT EXISTS idx_size_for_step_compression
        ON compression(step);
"""
sql_compression = """
    SELECT delta, name, mutate, step,
        AVG(size),
        AVG(encode_time),
        AVG(decode_time)
    FROM compression
    GROUP BY delta, name, mutate, step
    ORDER BY step ASC
"""

def all(raw, mutations, deltas, compressions):
    for m in mutations:
        for d in deltas:
            r = data(raw, mutate=m, delta=d)
            plot_all(r, m, d, 1)
        for c in compressions:
            r = data(raw, mutate=m, compression=c)
            plot_all(r, m, c, 0)


def plot(x, data, xlabel, ylabel, path):
    P.clf()
    f = P.figure()
    a = f.add_subplot(111)
    for label, y in data.iteritems():
        p, r, _, _, _ = np.polyfit(x, y, 1, full=True)
        P.plot(x, p[0] * x + p[1], '-', label=label)
        d = os.path.dirname(path)
        with open(os.path.join(d, label + '-residuals.txt'), 'w') as fd:
            fd.write('%.2f' % r[0])
    handles, labels = a.get_legend_handles_labels()
    a.legend(handles, labels)
    P.xlabel(xlabel)
    P.ylabel(ylabel)
    try:
        os.makedirs(os.path.dirname(path))
    except:
        pass
    f.savefig(path)
    P.close(f)


def data(raw, mutate=None, delta=None, compression=None):
    data = raw
    if mutate is not None:
        data = ifilter(lambda r: r[2] == mutate, data)
    if delta is not None:
        data = ifilter(lambda r: r[0] == delta, data)
    if compression is not None:
        data = ifilter(lambda r: r[1] == compression, data)
    data = list(data)
    if len(data[0]) == 7:
        combined = {}
        for r in data:
            if r[0] not in combined:
                combined[r[0]] = {}
            if r[1] not in combined[r[0]]:
                combined[r[0]][r[1]] = {}
            if r[2] not in combined[r[0]][r[1]]:
                combined[r[0]][r[1]][r[2]] = ([],[],[])
            combined[r[0]][r[1]][r[2]][0].append(r[4])
            combined[r[0]][r[1]][r[2]][1].append(r[5])
            combined[r[0]][r[1]][r[2]][2].append(r[6])
        data = []
        for d in combined:
            for c in combined[d]:
                for m in combined[d][c]:
                    data.append((d, c, m, combined[d][c][m]))
    return data


def plot_all(data, m, n, vary_by):
    x = np.array([run.__DATA_SIZE * i for i in xrange(len(data[0][3][0]))])
    size = {r[vary_by]: r[3][0] for r in data}
    encode = {r[vary_by]: r[3][1] for r in data}
    decode = {r[vary_by]: r[3][2] for r in data}
    plot(x, size, 'File Size (bytes)', 'Compressed Size (bytes)', 'images/all/size/%.2f/%s.png' % (m, n))
    plot(x, encode, 'File Size (bytes)', 'Encode Time (secs)', 'images/all/encode_time/%.2f/%s.png' % (m, n))
    plot(x, decode, 'File Size (bytes)', 'Decode Time (secs)', 'images/all/decode_time/%.2f/%s.png' % (m, n))


def best(raw, mutations, deltas, compressions):
    best = {
        'mutate 0 best by size': [
            (0.0, 'vcdiff', 'gzip'),
            (0.0, 'diff', 'zpaq'),
            (0.0, 'vcdiff', 'bzip2'),            
            (0.0, 'vcdiff', 'zpaq')],
        'mutate 0.01 best by size': [
            (0.01, 'vcdiff', 'bzip2'),
            (0.01, 'diff', 'zpaq'),
            (0.01, 'no_delta', 'gzip'),            
            (0.01, 'vcdiff', 'zpaq'),
            (0.01, 'no_delta', 'zpaq')],
        'mutate 0.05 best by size': [
            (0.05, 'no_delta', 'bzip2'),
            (0.05, 'diff', 'zpaq'),
            (0.05, 'diff', 'gzip'),
            (0.05, 'no_delta', 'gzip'),            
            (0.05, 'vcdiff', 'zpaq'),
            (0.05, 'vcdiff', 'gzip')],
        'mutate 0.1 best by size': [
            (0.1, 'no_delta', 'bzip2'),
            (0.1, 'diff', 'bzip2'),
            (0.1, 'diff', 'gzip'),
            (0.1, 'no_delta', 'gzip'),            
            (0.1, 'no_delta', 'zpaq'),
            (0.1, 'vcdiff', 'gzip')],
        'mutate 0.25 best by size': [
            (0.25, 'no_delta', 'bzip2'),
            (0.25, 'vcdiff', 'bzip2'),
            (0.25, 'vcdiff', 'zpaq'),
            (0.25, 'diff', 'gzip'),
            (0.25, 'no_delta', 'gzip'),            
            (0.25, 'no_delta', 'zpaq'),
            (0.25, 'vcdiff', 'gzip')],
        'mutate 0.5 best by size': [
            (0.5, 'no_delta', 'bzip2'),
            (0.5, 'vcdiff', 'bzip2'),
            (0.5, 'vcdiff', 'zpaq'),
            (0.5, 'diff', 'gzip'),
            (0.5, 'no_delta', 'gzip'),            
            (0.5, 'no_delta', 'zpaq'),
            (0.5, 'vcdiff', 'gzip')],
        'mutate 1.0 best by size': [
            (1.0, 'no_delta', 'bzip2'),
            (1.0, 'vcdiff', 'bzip2'),
            (1.0, 'vcdiff', 'zpaq'),
            (1.0, 'diff', 'gzip'),
            (1.0, 'diff', 'bzip2'),
            (1.0, 'diff', 'zpaq'),
            (1.0, 'no_delta', 'gzip'),            
            (1.0, 'no_delta', 'zpaq'),
            (1.0, 'vcdiff', 'gzip')],
        'vcdiff best by size': [
            (0.0, 'vcdiff', 'gzip'),
            (0.01, 'vcdiff', 'zpaq'),
            (0.05, 'vcdiff', 'gzip'),
            (0.05, 'vcdiff', 'zpaq'),
            (0.1, 'vcdiff', 'gzip'),
            (0.25, 'vcdiff', 'zpaq'),
            (0.5, 'vcdiff', 'zpaq'),
            (1.0, 'vcdiff', 'zpaq')],
        'vcdiff gzip over mutations': [
            (0.0, 'vcdiff', 'gzip'),
            (0.01, 'vcdiff', 'gzip'),
            (0.05, 'vcdiff', 'gzip'),
            (0.1, 'vcdiff', 'gzip'),
            (0.25, 'vcdiff', 'gzip'),
            (0.5, 'vcdiff', 'gzip'),
            (1.0, 'vcdiff', 'gzip'),]
    }
    for graph in best:
        size = {}
        encode = {}
        decode = {}
        for m, d, c in best[graph]:
            r = data(raw, mutate=m, delta=d, compression=c)
            size['%.2f-%s-%s' % (m, d, c)] = r[0][3][0]
            encode['%.2f-%s-%s' % (m, d, c)] = r[0][3][1]
            decode['%.2f-%s-%s' % (m, d, c)] = r[0][3][2]
        x = np.array([run.__DATA_SIZE * i for i in xrange(len(size.values()[0]))])
        plot(x, size, 'File Size (bytes)', 'Compressed Size (bytes)', 'images/best/%s/size.png' % graph)
        plot(x, encode, 'File Size (bytes)', 'Encode Time (secs)', 'images/best/%s/encode_time.png' % (graph))
        plot(x, decode, 'File Size (bytes)', 'Decode Time (secs)', 'images/best/%s/decode_time.png' % (graph))


def prep(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executescript(sql_index)
    conn.commit()

    cursor.execute('SELECT DISTINCT mutate from compression')
    mutations = [r[0] for r in cursor.fetchall()]
    cursor.execute('SELECT DISTINCT name from delta')
    deltas = [r[0] for r in cursor.fetchall()]
    cursor.execute('SELECT DISTINCT name from compression')
    compressions = [r[0] for r in cursor.fetchall()]

    cursor.execute(sql_compression)
    raw = cursor.fetchall()

    return raw, mutations, deltas, compressions

if __name__ == '__main__':
    DB_PATH = '/Users/william/Desktop/generated.db'
    raw, mutations, deltas, compressions = prep(DB_PATH)

    if len(sys.argv) == 1:
        all(raw, mutations, deltas, compressions)
    else:
        best(raw, mutations, deltas, compressions)
