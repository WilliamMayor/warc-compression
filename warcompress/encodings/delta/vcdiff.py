import os
import subprocess

from warcompress.encodings import utilities


def __vcdiff(source, target):
    with open(os.devnull, 'wb') as devnull:
        return subprocess.Popen(
            ['vcdiff', 'delta', '-dictionary', source, '-target', target],
            stdout=subprocess.PIPE,
            stderr=devnull
        ).communicate()[0]


def encode(data_path, iframe_every=0):
    return utilities.diff('vcdiff', __vcdiff, data_path, iframe_every)
