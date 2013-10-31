import os

from wand.image import Image
from nose.tools import assert_equals

from warcompress.modifiers.image import (
    ImageShrink
)

MODULE_PATH = os.path.realpath(__file__)
MODULE_DIR = os.path.dirname(MODULE_PATH)
DATA_PATH = os.path.join(MODULE_DIR, 'image.gif')


def test_shrink_default():
    h = 768
    w = 1024
    with open(DATA_PATH, 'rb') as fd:
        modifier = ImageShrink().modify(fd.read())
    count = 0
    for shrunk in modifier:
        with Image(blob=shrunk) as img:
            assert_equals(w, img.width)
            assert_equals(h, img.height)
        h = int(h * 0.9)
        w = int(w * 0.9)
        count += 1
    assert_equals(count, 21)


def test_shrink_more():
    h = 768
    w = 1024
    with open(DATA_PATH, 'rb') as fd:
        modifier = ImageShrink(percentage=0.2).modify(fd.read())
    for shrunk in modifier:
        with Image(blob=shrunk) as img:
            assert_equals(w, img.width)
            assert_equals(h, img.height)
        h = int(h * 0.8)
        w = int(w * 0.8)


def test_shrink_fewer_times():
    with open(DATA_PATH, 'rb') as fd:
        modifier = ImageShrink(count=10).modify(fd.read())
    assert_equals(11, len(list(modifier)))
