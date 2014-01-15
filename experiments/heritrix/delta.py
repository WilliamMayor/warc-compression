import os
import sqlite3
import hashlib
import base64
import subprocess
import tempfile

import utilities
from WARC import WARC

FIND_PREVIOUS_RESPONSES = ('SELECT record_id'
                           ' FROM record'
                           ' WHERE record_type = "response"'
                           '  AND uri = ?'
                           '  AND date < ?'
                           ' ORDER BY date DESC')

GET_LOCATION = ('SELECT path, offset'
                ' FROM LOCATION'
                ' WHERE record_id = ?')


def _bsdiff_patch(a, b):
    source = tempfile.NamedTemporaryFile()
    source.write(a)
    source.flush()
    target = tempfile.NamedTemporaryFile()
    target.write(b)
    target.flush()
    patch = tempfile.NamedTemporaryFile()
    with open(os.devnull, 'wb') as devnull:
        subprocess.call(
            ['bsdiff', source.name, target.name, patch.name],
            stdout=devnull,
            stderr=devnull
        )
    content = patch.read()
    source.close()
    target.close()
    patch.close()
    return content


def _diffe_patch(a, b):
    source = tempfile.NamedTemporaryFile()
    source.write(a)
    source.flush()
    target = tempfile.NamedTemporaryFile()
    target.write(b)
    target.flush()
    with open(os.devnull, 'wb') as devnull:
        content = subprocess.Popen(
            ['diff', '--ed', source.name, target.name],
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
            ['vcdiff', 'delta',
             '-dictionary', source.name,
             '-target', target.name],
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


def _find_older(headers, cursor):
    if headers['WARC-Type'] != 'response':
        return []
    return cursor.execute(
        FIND_PREVIOUS_RESPONSES, (
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
                               '%s@first-patch' % pname)
    }
    return headers, content

STRATEGIES = {
    'first': _first
}
PATCHERS = {
    'bsdiff': _bsdiff_patch
}


def all_the_things(from_dir, to_dir, index):
    cursor = sqlite3.connect(index).cursor()
    for root, dirs, files in os.walk(from_dir):
        for f in [f for f in files if f.endswith('.warc')]:
            abs_path = os.path.join(root, f)
            rel_path = abs_path.replace(from_dir, '', 1).lstrip('/')
            for headers, content, _ in WARC(abs_path).records():
                older = _find_older(headers, cursor)
                for pname, patcher in PATCHERS.iteritems():
                    for sname, strategy in STRATEGIES.iteritems():
                        n = '%s@%s' % (pname, sname)
                        p = os.path.join(to_dir, n, 'no_compression', rel_path)
                        w = WARC(p)
                        if len(older) > 0:
                            headers, content = strategy(cursor, headers, content, older, pname, patcher)  # NOQA
                        w.add_record(headers, content)
    return ['bsdiff@first']
