import os
import subprocess
import tempfile

from warcompress.encodings import utilities


def __bsdiff(source, target):
    patch = tempfile.NamedTemporaryFile()
    with open(os.devnull, 'wb') as devnull:
        subprocess.call(
            ['bsdiff', source, target, patch.name],
            stdout=devnull,
            stderr=devnull
        )
    text = patch.read()
    patch.close()
    return text


def encode(data_path, iframe_every=0):
    return utilities.diff('bsdiff', __bsdiff, data_path, iframe_every)
