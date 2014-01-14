"""
python compress_warcs.py ./original
"""

import hashlib
import base64
import sqlite3
import tempfile
import subprocess
import bz2
import zipfile
import gzip
import sys
import os

import utilities


FIND_PREVIOUS_RESPONSES = ('SELECT record_id'
                           ' FROM record'
                           ' WHERE record_type = \'response\''
                           '  AND uri = ?'
                           '  AND date < ?'
                           ' ORDER BY date DESC')

GET_LOCATION = ('SELECT path, offset'
                ' FROM LOCATION'
                ' WHERE record_id = ?')

FIND_CONCURRENT = ('SELECT `from`'
                   ' FROM concurrent'
                   ' WHERE `to` = ?')


def get_record(cursor, _id):
    location = cursor.execute(
        GET_LOCATION, (
            _id,
        )
    ).fetchone()
    with open(location[0], 'rb') as fd:
        fd.seek(location[1])
        return utilities.next_record(fd)[1]


def bsdiff_patch(a, b):
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


def diffe_patch(a, b):
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


def vcdiff_patch(a, b):
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


def delta(base_dir, warcs, index_path):
    conn = sqlite3.connect(index_path)
    cursor = conn.cursor()
    for old_path in warcs:
        print '\n', old_path
        for headers, content, _ in utilities.record_stream(old_path):
            previous = []
            if headers['WARC-Type'] == 'response':
                previous = cursor.execute(
                    FIND_PREVIOUS_RESPONSES, (
                        headers['WARC-Target-URI'],
                        headers['WARC-Date']
                    )
                ).fetchall()
            frames = {
                'first': None, 'previous': None,
                'iframe@2': None, 'iframe@5': None, 'iframe@10': None
            }
            if len(previous) > 0:
                frames['first'] = previous[0][0]
                frames['previous'] = previous[-1][0]
                index = len(previous)
                for iframe_every in [2, 5, 10]:
                    position = index - (index % iframe_every)
                    if position != index:
                        name = 'iframe@%d' % iframe_every
                        frames[name] = previous[position][0]
            for name, refers_to in frames.iteritems():
                previous_content = None
                if refers_to is not None:
                    previous_content = get_record(cursor, refers_to)
                for d in deltas:
                    new_path = os.path.join(
                        base_dir,
                        d,
                        name,
                        'raw',
                        os.path.basename(old_path)
                    )
                    try:
                        os.makedirs(os.path.dirname(new_path))
                    except:
                        pass
                    if previous_content is not None:
                        content = deltas[d](
                            previous_content,
                            content
                        )
                        pd = base64.b32encode(hashlib.sha1(content).digest())
                        headers = {
                            'WARC-Record-ID': headers['WARC-Record-ID'],
                            'Content-Length': str(len(content)),
                            'WARC-Date': headers['WARC-Date'],
                            'WARC-Type': 'revisit',
                            'WARC-Payload-Digest': 'sha1:' + pd,
                            'WARC-Refers-To': refers_to,
                            'WARC-Target-URI': headers['WARC-Target-URI'],
                            'WARC-Profile': ('http://netpreserve.org'
                                                   '/warc/1.0/revisit/'
                                                   '%s-patch' % d)
                        }
                    utilities.write_record(new_path, headers, content)
            sys.stdout.write('.')
            sys.stdout.flush()


def gz(to_dir, warcs, index_path):
    for old_path in warcs:
        new_path = os.path.join(to_dir, os.path.basename(old_path) + '.gz')
        gzw = gzip.open(new_path, 'w')
        with open(old_path, 'rb') as fd:
            gzw.write(fd.read())
        gzw.close()


def _zip(to_dir, warcs, index_path):
    for old_path in warcs:
        new_path = os.path.join(to_dir, os.path.basename(old_path) + '.zip')
        with zipfile.ZipFile(new_path, 'w', zipfile.ZIP_DEFLATED) as archive:
            archive.write(old_path)


def bzip2(to_dir, warcs, index_path):
    for old_path in warcs:
        new_path = os.path.join(to_dir, os.path.basename(old_path) + '.bz2')
        f_in = open(old_path, 'rb')
        f_out = bz2.BZ2File(new_path, 'w')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()


def list_warcs(_dir):
    warcs = []
    for (root, dirs, files) in os.walk(_dir):
        for f in files:
            if f.endswith('.warc'):
                warcs.append(os.path.join(root, f))
    return warcs


def list_raws(_dir):
    raws = []
    for (root, dirs, files) in os.walk(_dir):
        for d in dirs:
            if d == 'raw':
                raws.append(os.path.join(root, d))
    return raws

compression = {
    'gz': gz,
    'zip': _zip,
    'bzip2': bzip2
}
deltas = {
    'bsdiff': bsdiff_patch,
    'diffe': diffe_patch,
    'vcdiff': vcdiff_patch
}


if __name__ == '__main__':
    BASE_DIR = sys.argv[1]
    WARC_DIR = os.path.join(BASE_DIR, 'warc', 'raw')
    INDEX_PATH = os.path.join(BASE_DIR, 'warc', 'index.db')

    warcs = list_warcs(WARC_DIR)
    delta(BASE_DIR, warcs, INDEX_PATH)
    for raw in list_raws(BASE_DIR):
        warcs = list_warcs(raw)
        for name, func in compression.iteritems():
            to_path = os.path.join(os.path.dirname(raw), name)
            try:
                os.makedirs(to_path)
            except:
                pass
            func(to_path, warcs, INDEX_PATH)
