import os
import subprocess

from warcompress.encodings import utilities


def __diffe(source, target):
    with open(os.devnull, 'wb') as devnull:
        return subprocess.Popen(
            ['diff', '--ed', source, target],
            stdout=subprocess.PIPE,
            stderr=devnull
        ).communicate()[0]


def encode(data_path, iframe_every=0):
    return utilities.diff('diffe', __diffe, data_path, iframe_every)
