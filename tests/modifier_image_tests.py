import os

from wand.image import Image
from nose.tools import assert_equals

from warcompress.modifiers.image import (
    ImageScale,
    ImageRotate,
    ImageCrop,
    ImageGrayScale,
    ImageModulate
)

MODULE_PATH = os.path.realpath(__file__)
MODULE_DIR = os.path.dirname(MODULE_PATH)
DATA_PATH = os.path.join(MODULE_DIR, 'image.gif')

SAVE_RESULTS = True


def save(blob, name):
    if SAVE_RESULTS:
        with Image(blob=blob) as img:
            img.save(filename=name)


def test_scale_default():
    h = 768
    w = 1024
    with open(DATA_PATH, 'rb') as fd:
        modifier = ImageScale().modify(fd.read())
    count = 0
    for shrunk in modifier:
        with Image(blob=shrunk) as img:
            assert_equals(w, img.width)
            assert_equals(h, img.height)
        h = int(h * 0.9)
        w = int(w * 0.9)
        count += 1
    save(shrunk, 'small.gif')
    assert_equals(count, 21)


def test_scale_smaller():
    h = 768
    w = 1024
    with open(DATA_PATH, 'rb') as fd:
        modifier = ImageScale(percentage=0.8).modify(fd.read())
    for shrunk in modifier:
        with Image(blob=shrunk) as img:
            assert_equals(w, img.width)
            assert_equals(h, img.height)
        h = int(h * 0.8)
        w = int(w * 0.8)


def test_scale_larger():
    h = 768
    w = 1024
    with open(DATA_PATH, 'rb') as fd:
        modifier = ImageScale(percentage=1.01).modify(fd.read())
    for shrunk in modifier:
        with Image(blob=shrunk) as img:
            assert_equals(w, img.width)
            assert_equals(h, img.height)
        h = int(h * 1.01)
        w = int(w * 1.01)
    save(shrunk, 'large.gif')


def test_scale_fewer_times():
    with open(DATA_PATH, 'rb') as fd:
        modifier = ImageScale(count=10).modify(fd.read())
    assert_equals(11, len(list(modifier)))


def test_rotate():
    # This basically check that the methods run without failure
    with open(DATA_PATH, 'rb') as fd:
        modifier = ImageRotate(degrees=10, count=1).modify(fd.read())
    modifier.next()
    save(modifier.next(), 'rotated.gif')


def test_grayscale():
    # This basically check that the methods run without failure
    with open(DATA_PATH, 'rb') as fd:
        modifier = ImageGrayScale().modify(fd.read())
    modifier.next()
    save(modifier.next(), 'grayscale.gif')


def test_modulate():
    # This basically check that the methods run without failure
    with open(DATA_PATH, 'rb') as fd:
        modifier = ImageModulate().modify(fd.read())
    modifier.next()
    #save(modifier.next(), 'modulate.gif')


def test_crop():
    with open(DATA_PATH, 'rb') as fd:
        modifier = ImageCrop().modify(fd.read())
    for shrunk in modifier:
        pass
    save(shrunk, 'cropped.gif')
