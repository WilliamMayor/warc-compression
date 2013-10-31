import os
import zipfile
import gzip
import bz2
import subprocess

from nose.tools import assert_equal, assert_true
from warcompress.encodings import utilities
from warcompress.encodings.compression import (
    _zip,
    gz,
    bzip2
)
from warcompress.encodings.delta import (
    bsdiff,
    diffe,
    diffe_gz,
    vcdiff
)

MODULE_PATH = os.path.realpath(__file__)
MODULE_DIR = os.path.dirname(MODULE_PATH)
DATA_PATH = os.path.join(MODULE_DIR, 'data.txt')

orig_text = None
new_files = None


def setup():
    global new_files, orig_text
    new_files = []
    with open(DATA_PATH, 'r') as fd:
        orig_text = fd.read()


def teardown():
    for f in new_files:
        try:
            os.remove(f)
        except:
            pass


def test_zip():
    zipped_path = _zip.encode(DATA_PATH)
    new_files.append(zipped_path)
    with zipfile.ZipFile(zipped_path, 'r', zipfile.ZIP_DEFLATED) as archive:
        l = archive.infolist()
        text = archive.read(l[0])
        assert_equal(text, orig_text)


def test_gzip():
    zipped_path = gz.encode(DATA_PATH)
    new_files.append(zipped_path)
    with gzip.open(zipped_path, 'r') as archive:
        text = archive.read()
        assert_equal(text, orig_text)


def test_bz2():
    zipped_path = bzip2.encode(DATA_PATH)
    new_files.append(zipped_path)
    with bz2.BZ2File(zipped_path, 'r') as archive:
        text = archive.read()
        assert_equal(text, orig_text)


def test_pair_0():
    assert_equal(
        [('0', ['1', '2', '3', '4', '5'])],
        utilities.__pair(['0', '1', '2', '3', '4', '5'], 0)
    )


def test_pair_1():
    assert_equal(
        [('0', ['1']),
         ('1', ['2']),
         ('2', ['3']),
         ('3', ['4']),
         ('4', ['5']),
         ('5', [])],
        utilities.__pair(['0', '1', '2', '3', '4', '5'], 1)
    )


def test_pair_2():
    assert_equal(
        [('0', ['1']),
         ('2', ['3']),
         ('4', ['5'])],
        utilities.__pair(['0', '1', '2', '3', '4', '5'], 2)
    )


def test_pair_3():
    assert_equal(
        [('0', ['1', '2']),
         ('3', ['4', '5'])],
        utilities.__pair(['0', '1', '2', '3', '4', '5'], 3)
    )


def test_pair_4():
    assert_equal(
        [('0', ['1', '2', '3']),
         ('4', ['5'])],
        utilities.__pair(['0', '1', '2', '3', '4', '5'], 4)
    )


def test_pair_5():
    assert_equal(
        [('0', ['1', '2', '3', '4']),
         ('5', [])],
        utilities.__pair(['0', '1', '2', '3', '4', '5'], 5)
    )


def test_pair_6():
    assert_equal(
        [('0', ['1', '2', '3', '4', '5'])],
        utilities.__pair(['0', '1', '2', '3', '4', '5'], 6)
    )


def diff_patch(base, patch):
    base_path = os.path.join(MODULE_DIR, 'patch_base')
    with open(base_path, 'w') as fd:
        fd.write(base)
    new_files.append(base_path)
    patch_path = os.path.join(MODULE_DIR, 'patch')
    with open(patch_path, 'w') as fd:
        fd.write(patch)
    new_files.append(patch_path)
    subprocess.call(
        ['patch --ed %s %s' % (base_path, patch_path)],
        shell=True,
        stderr=subprocess.PIPE
    )
    with open(base_path, 'r') as fd:
        return fd.read()[0:-1]


def test_diffe_from_first():
    diffed_file = diffe.encode(DATA_PATH, iframe_every=0)
    new_files.append(diffed_file)
    with open(diffed_file, 'r') as fd:
        files = fd.read()
        files = files.split(utilities.RECORD_SEPARATOR)
        restored_text = files[0]
        for f in files[1:]:
            restored_text += utilities.RECORD_SEPARATOR
            restored_text += diff_patch(files[0], f)
        assert_equal(restored_text, orig_text)


def test_diffe_from_previous():
    diffed_file = diffe.encode(DATA_PATH, iframe_every=1)
    new_files.append(diffed_file)
    with open(diffed_file, 'r') as fd:
        files = fd.read()
        files = files.split(utilities.RECORD_SEPARATOR)
        restored_text = files[0]
        previous = restored_text
        for f in files[1:]:
            restored_text += utilities.RECORD_SEPARATOR
            current = diff_patch(previous, f)
            restored_text += current
            previous = current
        assert_equal(restored_text, orig_text)


def tedst_diff_iframe_every_three():
    diffed_file = diffe.encode(DATA_PATH, iframe_every=3)
    new_files.append(diffed_file)
    with open(diffed_file, 'r') as fd:
        files = fd.read()
        files = files.split(utilities.RECORD_SEPARATOR)
        restored_text = files[0]
        restored_text += utilities.RECORD_SEPARATOR
        restored_text += diff_patch(files[0], files[1])
        restored_text += utilities.RECORD_SEPARATOR
        restored_text += diff_patch(files[0], files[2])
        for i in xrange(3, len(files), 3):
            restored_text += utilities.RECORD_SEPARATOR
            restored_text += files[i]
            try:
                text = diff_patch(files[i], files[i+1])
                restored_text += utilities.RECORD_SEPARATOR
                restored_text += text
                text = diff_patch(files[i], files[i+2])
                restored_text += utilities.RECORD_SEPARATOR
                restored_text += text
            except IndexError:
                pass
        assert_equal(restored_text, orig_text)


def test_diff_gzip():
    path = diffe_gz.encode(DATA_PATH, iframe_every=3)
    new_files.append(path)
    new_files.append(path[0:-3])
    assert_true('i3.diffe.gz' in path)


def vcdiff_patch(source, delta):
    base_path = os.path.join(MODULE_DIR, 'vcpatch_base')
    with open(base_path, 'w') as fd:
        fd.write(source)
    new_files.append(base_path)
    patch_path = os.path.join(MODULE_DIR, 'vcpatch')
    with open(patch_path, 'w') as fd:
        fd.write(delta)
    new_files.append(patch_path)
    return subprocess.check_output(
        ['vcdiff patch -dictionary %s -delta %s' % (base_path, patch_path)],
        shell=True,
        stderr=subprocess.PIPE
    )


def test_vcdiff():
    diffed_file = vcdiff.encode(DATA_PATH, iframe_every=0)
    new_files.append(diffed_file)
    with open(diffed_file, 'r') as fd:
        files = fd.read()
        files = files.split(utilities.RECORD_SEPARATOR)
        restored_text = files[0]
        for f in files[1:]:
            restored_text += utilities.RECORD_SEPARATOR
            restored_text += vcdiff_patch(files[0], f)
        assert_equal(restored_text, orig_text)


def bsdiff_patch(source, delta):
    base_path = os.path.join(MODULE_DIR, 'patch_base')
    with open(base_path, 'w') as fd:
        fd.write(source)
    new_files.append(base_path)
    patch_path = os.path.join(MODULE_DIR, 'patch')
    with open(patch_path, 'w') as fd:
        fd.write(delta)
    new_files.append(patch_path)
    target_path = os.path.join(MODULE_DIR, 'target')
    new_files.append(target_path)
    subprocess.call(
        ['bspatch %s %s %s' % (base_path, target_path, patch_path)],
        shell=True,
        stderr=subprocess.PIPE
    )
    with open(target_path, 'r') as fd:
        return fd.read()


def test_bsdiff():
    diffed_file = bsdiff.encode(DATA_PATH, iframe_every=0)
    new_files.append(diffed_file)
    with open(diffed_file, 'r') as fd:
        files = fd.read()
        files = files.split(utilities.RECORD_SEPARATOR)
        restored_text = files[0]
        for f in files[1:]:
            restored_text += utilities.RECORD_SEPARATOR
            restored_text += bsdiff_patch(files[0], f)
        assert_equal(restored_text, orig_text)
