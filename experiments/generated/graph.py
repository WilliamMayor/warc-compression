import math
import os
import sqlite3

from collections import defaultdict

import pylab as P

import run

sql_index = """
    CREATE INDEX IF NOT EXISTS idx_size_for_step_compression
        ON compression(delta, name, mutate, step);
    CREATE INDEX IF NOT EXISTS idx_size_for_step_compression
        ON compression(step);
"""
sql_compression = """
    SELECT delta, name, mutate, step,
        AVG(size), STDDEV(size),
        AVG(encode_time), STDDEV(encode_time),
        AVG(decode_time), STDDEV(decode_time)
    FROM compression
    GROUP BY delta, name, mutate, step
    ORDER BY step ASC
"""
sql_delta = """
    SELECT name, '', mutate, step,
        AVG(size), STDDEV(size),
        AVG(encode_time), STDDEV(encode_time),
        AVG(decode_time), STDDEV(decode_time)
    FROM delta
    GROUP BY name, mutate, step
    ORDER BY step ASC
"""

sql_select_d_c = """
    SELECT mutate, step,
        AVG(size), STDDEV(size),
        AVG(time_taken), STDDEV(time_taken)
    FROM entry
    WHERE delta = ?
        AND compression = ?
    GROUP BY mutate, step
"""
sql_select_m_d = """
    SELECT compression, step,
        AVG(size), STDDEV(size),
        AVG(time_taken), STDDEV(time_taken)
    FROM entry
    WHERE mutate = ?
        AND delta = ?
    GROUP BY compression, step
"""
sql_select_m_c = """
    SELECT delta, step,
        AVG(size), STDDEV(size),
        AVG(time_taken), STDDEV(time_taken)
    FROM entry
    WHERE mutate = ?
        AND compression = ?
    GROUP BY delta, step
"""


class StdDev:
    def __init__(self):
        self.m = 0.0
        self.s = 0.0
        self.k = 1

    def step(self, value):
        _m = self.m
        self.m += (value - _m) / self.k
        self.s += (value - _m) * (value - self.m)
        self.k += 1

    def finalize(self):
        return math.sqrt(self.s / (self.k - 1))


DB_PATH = os.path.join(run.HOME_DIR, 'results.db')
conn = sqlite3.connect(DB_PATH)
conn.create_aggregate("STDDEV", 1, StdDev)
cursor = conn.cursor()
cursor.executescript(sql_index)
conn.commit()

deltas = [d[0].__name__ for d in run.deltas]
compressions = [c[0].__name__ for c in run.compressions]

cursor.execute(sql_compression)
raw = cursor.fetchall()

for m in list(set(map(lambda r: r[2], raw))):
    mdata = filter(lambda r: r[2] == m, raw)
    for d in deltas:
        ddata = filter(lambda r: r[0] == d, mdata)
        x = sorted(list(set(map(lambda r: r[3] * run.DATA_SIZE, ddata))))

        P.clf()
        f = P.figure()
        a = f.add_subplot(111)
        sizes = defaultdict(list)
        for r in ddata:
            sizes[r[1]].append(r[4])
        for i, y in enumerate(sizes.values()):
            a.plot(x, y, label=sizes.keys()[i])
        handles, labels = a.get_legend_handles_labels()
        a.legend(handles, labels)
        P.xlabel('File Size (bytes)')
        P.ylabel('Compressed Size (bytes)')
        f.savefig('images/size-%.2f-%s.png' % (m, d))
        P.close(f)

        P.clf()
        f = P.figure()
        a = f.add_subplot(111)
        encode_time = defaultdict(list)
        for r in ddata:
            encode_time[r[1]].append(abs(r[6]))
        for i, y in enumerate(encode_time.values()):
            a.plot(x, y, label=encode_time.keys()[i])
        handles, labels = a.get_legend_handles_labels()
        a.legend(handles, labels)
        P.xlabel('File Size (bytes)')
        P.ylabel('Encoding Time (secs)')
        f.savefig('images/encode_time-%.2f-%s.png' % (m, d))
        P.close(f)

        P.clf()
        f = P.figure()
        a = f.add_subplot(111)
        decode_time = defaultdict(list)
        for r in ddata:
            decode_time[r[1]].append(abs(r[8]))
        for i, y in enumerate(decode_time.values()):
            a.plot(x, y, label=decode_time.keys()[i])
        handles, labels = a.get_legend_handles_labels()
        a.legend(handles, labels)
        P.xlabel('File Size (bytes)')
        P.ylabel('Decoding Time (secs)')
        f.savefig('images/decode_time-%.2f-%s.png' % (m, d))
        P.close(f)

    for c in compressions:
        cdata = filter(lambda r: r[1] == c, mdata)
        x = sorted(list(set(map(lambda r: r[3] * run.DATA_SIZE, cdata))))

        P.clf()
        f = P.figure()
        a = f.add_subplot(111)
        sizes = defaultdict(list)
        for r in cdata:
            sizes[r[0]].append(r[4])
        for i, y in enumerate(sizes.values()):
            a.plot(x, y, label=sizes.keys()[i])
        handles, labels = a.get_legend_handles_labels()
        a.legend(handles, labels)
        P.xlabel('File Size (bytes)')
        P.ylabel('Compressed Size (bytes)')
        f.savefig('images/size-%.2f-%s.png' % (m, c))
        P.close(f)
        P.close(f)

        P.clf()
        f = P.figure()
        a = f.add_subplot(111)
        encode_time = defaultdict(list)
        for r in cdata:
            encode_time[r[0]].append(abs(r[6]))
        for i, y in enumerate(encode_time.values()):
            a.plot(x, y, label=encode_time.keys()[i])
        handles, labels = a.get_legend_handles_labels()
        a.legend(handles, labels)
        P.xlabel('File Size (bytes)')
        P.ylabel('Encoding Time (secs)')
        f.savefig('images/encode_time-%.2f-%s.png' % (m, c))
        P.close(f)

        P.clf()
        f = P.figure()
        a = f.add_subplot(111)
        decode_time = defaultdict(list)
        for r in cdata:
            decode_time[r[0]].append(abs(r[8]))
        for i, y in enumerate(decode_time.values()):
            a.plot(x, y, label=decode_time.keys()[i])
        handles, labels = a.get_legend_handles_labels()
        a.legend(handles, labels)
        P.xlabel('File Size (bytes)')
        P.ylabel('Decoding Time (secs)')
        f.savefig('images/decode_time-%.2f-%s.png' % (m, c))
        P.close(f)
