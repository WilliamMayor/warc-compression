import os
import zipfile
import gzip
import bz2
import subprocess

from nose.tools import assert_equal, assert_true
import warcompress.experiments.encodings as encodings

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
    zipped_path = encodings._zip(DATA_PATH)
    new_files.append(zipped_path)
    with zipfile.ZipFile(zipped_path, 'r', zipfile.ZIP_DEFLATED) as archive:
        l = archive.infolist()
        text = archive.read(l[0])
        assert_equal(text, orig_text)


def test_gzip():
    zipped_path = encodings._gzip(DATA_PATH)
    new_files.append(zipped_path)
    with gzip.open(zipped_path, 'r') as archive:
        text = archive.read()
        assert_equal(text, orig_text)


def test_bz2():
    zipped_path = encodings._bz2(DATA_PATH)
    new_files.append(zipped_path)
    with bz2.BZ2File(zipped_path, 'r') as archive:
        text = archive.read()
        assert_equal(text, orig_text)


def patch(base, patch):
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


def test_diff_from_first():
    diffed_file = encodings.diffe(DATA_PATH, iframe_every=0, waterfall=False)
    new_files.append(diffed_file)
    with open(diffed_file, 'r') as fd:
        files = fd.read()
        files = files.split(encodings.RECORD_SEPARATOR)
        restored_text = files[0]
        for f in files[1:]:
            restored_text += encodings.RECORD_SEPARATOR
            restored_text += patch(files[0], f)
        assert_equal(restored_text, orig_text)


def test_diff_from_previous():
    diffed_file = encodings.diffe(DATA_PATH, iframe_every=0, waterfall=True)
    new_files.append(diffed_file)
    with open(diffed_file, 'r') as fd:
        files = fd.read()
        files = files.split(encodings.RECORD_SEPARATOR)
        restored_text = files[0]
        previous = restored_text
        for f in files[1:]:
            restored_text += encodings.RECORD_SEPARATOR
            current = patch(previous, f)
            restored_text += current
            previous = current
        assert_equal(restored_text, orig_text)


def test_diff_iframe_every_three_waterfall():
    diffed_file = encodings.diffe(DATA_PATH, iframe_every=3, waterfall=True)
    new_files.append(diffed_file)
    with open(diffed_file, 'r') as fd:
        files = fd.read()
        files = files.split(encodings.RECORD_SEPARATOR)
        restored_text = files[0]
        restored_text += encodings.RECORD_SEPARATOR
        step = patch(files[0], files[1])
        restored_text += step
        restored_text += encodings.RECORD_SEPARATOR
        restored_text += patch(step, files[2])
        for i in xrange(3, len(files), 3):
            restored_text += encodings.RECORD_SEPARATOR
            restored_text += files[i]
            try:
                step = patch(files[i], files[i+1])
                restored_text += encodings.RECORD_SEPARATOR
                restored_text += step
                text = patch(step, files[i+2])
                restored_text += encodings.RECORD_SEPARATOR
                restored_text += text
            except IndexError:
                pass
        assert_equal(restored_text, orig_text)


def test_diff_iframe_every_three_from_iframe():
    diffed_file = encodings.diffe(DATA_PATH, iframe_every=3, waterfall=False)
    new_files.append(diffed_file)
    with open(diffed_file, 'r') as fd:
        files = fd.read()
        files = files.split(encodings.RECORD_SEPARATOR)
        restored_text = files[0]
        restored_text += encodings.RECORD_SEPARATOR
        restored_text += patch(files[0], files[1])
        restored_text += encodings.RECORD_SEPARATOR
        restored_text += patch(files[0], files[2])
        for i in xrange(3, len(files), 3):
            restored_text += encodings.RECORD_SEPARATOR
            restored_text += files[i]
            try:
                text = patch(files[i], files[i+1])
                restored_text += encodings.RECORD_SEPARATOR
                restored_text += text
                text = patch(files[i], files[i+2])
                restored_text += encodings.RECORD_SEPARATOR
                restored_text += text
            except IndexError:
                pass
        assert_equal(restored_text, orig_text)


def test_diff_gzip():
    path = encodings.diffe_gzip(DATA_PATH, iframe_every=3, waterfall=False)
    new_files.append(path)
    assert_true('i.3.diff.gz' in path)
