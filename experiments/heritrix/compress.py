import os
import tarfile
import gzip
import zipfile
import shutil
from bz2 import BZ2File

import meta
from utilities import ensure_dirs, progress


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


def zip_(from_dir, to_dir):
    to = os.path.join(to_dir, 'zip')
    for rel_path in _relative_paths(from_dir):
        old_path = os.path.join(from_dir, rel_path)
        new_path = os.path.join(to, rel_path) + '.zip'
        ensure_dirs(new_path)
        with zipfile.ZipFile(new_path, 'w', zipfile.ZIP_DEFLATED) as archive:
            archive.write(old_path)
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


def all_the_things(from_dir, to_dir, index_path):
    progress('compressing', end=True)
    for f in [gzip_, zip_, bzip2, targz, tarbzip2]:
        compressed = f(from_dir, to_dir)
        meta.record_sizes(compressed, index_path)
        try:
            os.remove(compressed)
        except OSError:
            shutil.rmtree(compressed)

if __name__ == '__main__':
    import sys
    all_the_things(sys.argv[1], sys.argv[2])