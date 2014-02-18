import sys
import os
import sqlite3
import hashlib
import base64
import subprocess
import tempfile
from itertools import izip
from collections import defaultdict

from WARC import WARC

BSDIFF_DIR = '/home/wmayor/wc/bsdiff-4.3/'
VCDIFF_DIR = '/home/wmayor/wc/open-vcdiff-0.8.3/'
BSDIFF_DIR = '/usr/local/bin/'
VCDIFF_DIR = '/usr/local/bin/'


SIZE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS recordsize(
        record_id TEXT,
        content_length INTEGER
    );
"""
FIND_PREVIOUS_RESPONSES = """
    SELECT record_id
    FROM record
    WHERE record_type = "response"
        AND job = ?
        AND uri = ?
        AND date < ?
    ORDER BY date ASC
"""
INSERT_RECORD_SIZE = """
    INSERT INTO
    recordsize (record_id, content_length)
    VALUES (?, ?)
"""
GET_LOCATION = """
    SELECT path, offset
    FROM record
    WHERE record_id = ?
"""


def _bsdiff_diff(a, b):
    source = tempfile.NamedTemporaryFile()
    source.write(a)
    source.flush()
    target = tempfile.NamedTemporaryFile()
    target.write(b)
    target.flush()
    patch = tempfile.NamedTemporaryFile()
    with open(os.devnull, 'wb') as devnull:
        subprocess.call(
            ['%sbsdiff' % BSDIFF_DIR, source.name, target.name, patch.name],
            stdout=devnull,
            stderr=devnull
        )
    content = patch.read()
    source.close()
    target.close()
    patch.close()
    return content


def _bsdiff_patch(a, b):
    source = tempfile.NamedTemporaryFile()
    source.write(a)
    source.flush()
    patch = tempfile.NamedTemporaryFile()
    patch.write(b)
    patch.flush()
    target = tempfile.NamedTemporaryFile()
    with open(os.devnull, 'wb') as devnull:
        subprocess.call(
            ['%sbspatch' % BSDIFF_DIR, source.name, target.name, patch.name],
            stdout=devnull,
            stderr=devnull
        )
    content = target.read()
    source.close()
    target.close()
    patch.close()
    return content


def _diffe_diff(a, b):
    source = tempfile.NamedTemporaryFile()
    source.write(a)
    source.flush()
    target = tempfile.NamedTemporaryFile()
    target.write(b)
    target.flush()
    with open(os.devnull, 'wb') as devnull:
        content = subprocess.Popen(
            ['diff', '--text', '--ed', source.name, target.name],
            stdout=subprocess.PIPE,
            stderr=devnull
        ).communicate()[0]
    source.close()
    target.close()
    return content


def _diffe_patch(a, b):
    source = tempfile.NamedTemporaryFile()
    source.write(a)
    source.flush()
    with open(os.devnull, 'wb') as devnull:
        subprocess.Popen(
            ['ed', source.name],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=devnull
        ).communicate(input=b+'\nw\n')
    source.seek(0)
    content = source.read().rstrip()
    source.close()
    return content


def _vcdiff_diff(a, b):
    source = tempfile.NamedTemporaryFile()
    source.write(a)
    source.flush()
    target = tempfile.NamedTemporaryFile()
    target.write(b)
    target.flush()
    with open(os.devnull, 'wb') as devnull:
        content = subprocess.Popen(
            ['%svcdiff' % VCDIFF_DIR, 'delta',
             '-dictionary', source.name,
             '-target', target.name],
            stdout=subprocess.PIPE,
            stderr=devnull
        ).communicate()[0]
    source.close()
    target.close()
    return content


def _vcdiff_patch(a, b):
    source = tempfile.NamedTemporaryFile()
    source.write(a)
    source.flush()
    target = tempfile.NamedTemporaryFile()
    target.write(b)
    target.flush()
    with open(os.devnull, 'wb') as devnull:
        content = subprocess.Popen(
            ['%svcdiff' % VCDIFF_DIR, 'patch',
             '-dictionary', source.name,
             '-delta', target.name],
            stdout=subprocess.PIPE,
            stderr=devnull
        ).communicate()[0]
    source.close()
    target.close()
    return content


def _get_record(cursor, _id):
    location = cursor.execute(
        GET_LOCATION, (
            _id,
        )
    ).fetchone()
    return WARC(location[0]).get_record(int(location[1]))


def _find_older(headers, cursor, job):
    if headers['WARC-Type'] != 'response':
        return []
    return cursor.execute(
        FIND_PREVIOUS_RESPONSES, (
            job,
            headers['WARC-Target-URI'],
            headers['WARC-Date']
        )).fetchall()


def _first(cursor, headers, content, older, pname, patcher):
    o_headers, o_content = _get_record(cursor, older[0][0])
    content = patcher(o_content, content)
    sha = base64.b32encode(hashlib.sha1(content).digest())
    headers = {
        'WARC-Record-ID': headers['WARC-Record-ID'],
        'Content-Length': str(len(content)),
        'WARC-Date': headers['WARC-Date'],
        'WARC-Type': 'revisit',
        'WARC-Payload-Digest': 'sha1:' + sha,
        'WARC-Refers-To': o_headers['WARC-Record-ID'],
        'WARC-Target-URI': headers['WARC-Target-URI'],
        'WARC-Profile': ('http://netpreserve.org'
                               '/warc/1.0/revisit/'
                               '%s@first-delta' % pname)
    }
    return headers, content


def _previous(cursor, headers, content, older, pname, patcher):
    o_headers, o_content = _get_record(cursor, older[-1][0])
    content = patcher(o_content, content)
    sha = base64.b32encode(hashlib.sha1(content).digest())
    headers = {
        'WARC-Record-ID': headers['WARC-Record-ID'],
        'Content-Length': str(len(content)),
        'WARC-Date': headers['WARC-Date'],
        'WARC-Type': 'revisit',
        'WARC-Payload-Digest': 'sha1:' + sha,
        'WARC-Refers-To': o_headers['WARC-Record-ID'],
        'WARC-Target-URI': headers['WARC-Target-URI'],
        'WARC-Profile': ('http://netpreserve.org'
                               '/warc/1.0/revisit/'
                               '%s@previous-delta' % pname)
    }
    return headers, content


def _iframe(every):
    def inner(cursor, headers, content, older, pname, patcher):
        remainder = len(older) % every
        if remainder == 0:
            return headers, content
        o_headers, o_content = _get_record(cursor, older[-remainder][0])
        content = patcher(o_content, content)
        sha = base64.b32encode(hashlib.sha1(content).digest())
        headers = {
            'WARC-Record-ID': headers['WARC-Record-ID'],
            'Content-Length': str(len(content)),
            'WARC-Date': headers['WARC-Date'],
            'WARC-Type': 'revisit',
            'WARC-Payload-Digest': 'sha1:' + sha,
            'WARC-Refers-To': o_headers['WARC-Record-ID'],
            'WARC-Target-URI': headers['WARC-Target-URI'],
            'WARC-Profile': ('http://netpreserve.org'
                                   '/warc/1.0/revisit/'
                                   '%s@%d-delta' % (pname, every))
        }
        return headers, content
    return inner


STRATEGIES = {
    'first': _first,
    'previous': _previous,
    '10': _iframe(10)
}
PATCHERS = {
    'bsdiff': _bsdiff_diff,
    'diffe': _diffe_diff,
    'vcdiff': _vcdiff_diff
}
NAMES = ['%s@%s' % (x, y) for x in PATCHERS.keys() for y in STRATEGIES.keys()]


def run(warcs_dir, scratch_dir, db_dir, job):
    conn = sqlite3.connect(os.path.join(db_dir, 'index.db'))
    cursor = conn.cursor()
    inserts = defaultdict(list)
    for root, dirs, files in os.walk(warcs_dir):
        for f in [f for f in files if f.endswith('.warc')]:
            abs_path = os.path.join(root, f)
            for headers, content, _ in WARC(abs_path):
                older = _find_older(headers, cursor, job)
                for pname, patcher in PATCHERS.iteritems():
                    for sname, strategy in STRATEGIES.iteritems():
                        n = '%s@%s' % (pname, sname)
                        nc = os.path.join(scratch_dir, n, 'no_compression')
                        rel_path = abs_path.replace(warcs_dir, '', 1)
                        rel_path = rel_path.lstrip('/')
                        p = os.path.join(nc, rel_path)
                        w = WARC(p)
                        if len(older) > 0:
                            d_headers, d_content = strategy(
                                cursor, headers, content,
                                older, pname, patcher)
                            w.add_record(d_headers, d_content)
                            inserts[n].append((
                                d_headers['WARC-Record-ID'], len(d_content)))
                        else:
                            w.add_record(headers, content)
    conn.close()
    for n, i in inserts.iteritems():
        conn = sqlite3.connect(os.path.join(db_dir, '%s.db' % n))
        cursor = conn.cursor()
        cursor.executescript(SIZE_SCHEMA)
        cursor.executemany(INSERT_RECORD_SIZE, i)
        conn.commit()
        conn.close()
    return NAMES


def _test_unpatch(headers, content, index_path):
    if 'delta' not in headers['WARC-Profile']:
        return content
    conn = sqlite3.connect(index_path)
    cursor = conn.cursor()
    refers_to = headers['WARC-Refers-To']
    o_headers, o_content = _get_record(cursor, refers_to)
    if o_headers['WARC-Type'] == 'revisit':
        o_content = _test_unpatch(o_headers, o_content, index_path)
    conn.close()
    if 'bsdiff' in headers['WARC-Profile']:
        d_content = _bsdiff_patch(o_content, content)
    elif 'vcdiff' in headers['WARC-Profile']:
        d_content = _vcdiff_patch(o_content, content)
    elif 'diffe' in headers['WARC-Profile']:
        d_content = _diffe_patch(o_content, content)
    return d_content


def test(original_path, diffed_path, index_path):
    o_warc = WARC(original_path)
    d_warc = WARC(diffed_path)
    for o_record, d_record in izip(o_warc.records(), d_warc.records()):
        assert o_record[0]['WARC-Record-ID'] == d_record[0]['WARC-Record-ID']
        if d_record[0]['WARC-Type'] == 'revisit':
            d_content = _test_unpatch(d_record[0], d_record[1], index_path)
            if d_content.strip() == o_record[1].strip():
                sys.stdout.write('.')
            else:
                sys.stdout.write('w')
            sys.stdout.flush()
    sys.stdout.write('\n')


if __name__ == '__main__':
    a = 'one\ntwo\nthree'
    b = 'two\nthree\nfour'
    assert _bsdiff_patch(a, _bsdiff_diff(a, b)) == b
    assert _vcdiff_patch(a, _vcdiff_diff(a, b)) == b
    assert _diffe_patch(a, _diffe_diff(a, b)) == b
    data_root = sys.argv[1]
    index_root = sys.argv[2]
    no_delta = os.path.join(data_root, 'no_delta', 'no_compression')
    for d in [d for d in os.walk(data_root).next()[1] if d != 'no_delta']:
        print('Testing', d)
        index_path = os.path.join(index_root, d, 'index.db')
        delta = os.path.join(data_root, d, 'no_compression')
        for f in [f for f in os.walk(delta).next()[2] if f.endswith('.warc')]:
            print('  ' + f)
            o_path = os.path.join(no_delta, f)
            d_path = os.path.join(data_root, d, 'no_compression', f)
            test(o_path, d_path, index_path)
