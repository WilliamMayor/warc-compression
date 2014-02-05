import sqlite3
import os
import tarfile
import gzip
import shutil
from bz2 import BZ2File

SIZE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS archivesize(
        compression_type TEXT,
        path TEXT,
        size INTEGER
    );
"""
INSERT_ARCHIVE_SIZE = """
    INSERT INTO
    archivesize (compression_type, path, size)
    VALUES (?, ?, ?)
"""


def ensure_dirs(path):
    try:
        os.makedirs(os.path.dirname(path))
    except:
        pass


def _relative_paths(from_dir):
    for root, _, files in os.walk(from_dir):
        for f in files:
            abs_path = os.path.join(root, f)
            yield abs_path.replace(from_dir, '', 1).lstrip('/')


def gzip_(from_dir, to_dir):
    to = os.path.join(to_dir, 'gzip')
    for rel_path in _relative_paths(from_dir):
        old_path = os.path.join(from_dir, rel_path)
        new_path = os.path.join(to, rel_path) + '.gz'
        ensure_dirs(new_path)
        gzw = gzip.open(new_path, 'w')
        with open(old_path, 'rb') as fd:
            gzw.write(fd.read())
        gzw.close()
    return to


def bzip2(from_dir, to_dir):
    to = os.path.join(to_dir, 'bzip2')
    for rel_path in _relative_paths(from_dir):
        old_path = os.path.join(from_dir, rel_path)
        new_path = os.path.join(to, rel_path) + '.bz2'
        ensure_dirs(new_path)
        f_in = open(old_path, 'rb')
        f_out = BZ2File(new_path, 'w')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
    return to


def targz(from_dir, to_dir):
    to = os.path.join(to_dir, 'archive.tar.gz')
    ensure_dirs(to)
    tar = tarfile.open(to, "w:gz")
    for rel_path in _relative_paths(from_dir):
        old_path = os.path.join(from_dir, rel_path)
        tar.add(name=old_path, arcname=rel_path)
    tar.close()
    return to


def tarbzip2(from_dir, to_dir):
    to = os.path.join(to_dir, 'archive.tar.bz2')
    ensure_dirs(to)
    tar = tarfile.open(to, "w:bz2")
    for rel_path in _relative_paths(from_dir):
        old_path = os.path.join(from_dir, rel_path)
        tar.add(name=old_path, arcname=rel_path)
    tar.close()
    return to


def run(names, data_dir, db_dir):
    strats = {
        'gzip': gzip_, 'bz2': bzip2,
        'tar.gz': targz, 'tar.bz2': tarbzip2}
    for dn in names:
        original_path = os.path.join(data_dir, dn, 'no_compression')
        inserts = []
        for r, _, files in os.walk(original_path):
            for f in files:
                path = os.path.join(r, f)
                size = os.path.getsize(path)
                inserts.append(('no_compression', path, size))
        for cn, func in strats.iteritems():
            compressed = func(
                original_path,
                os.path.join(data_dir, dn))
            if os.path.isdir(compressed):
                for r, _, files in os.walk(compressed):
                    for f in files:
                        path = os.path.join(r, f)
                        size = os.path.getsize(path)
                        inserts.append((cn, path, size))
                shutil.rmtree(compressed)
            else:
                size = os.path.getsize(compressed)
                inserts.append((cn, compressed, size))
                os.remove(compressed)
        db_path = os.path.join(db_dir, '%s.db' % dn)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.executescript(SIZE_SCHEMA)
        cursor.executemany(INSERT_ARCHIVE_SIZE, inserts)
        conn.commit()
        conn.close()
