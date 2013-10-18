import os

from warcompress.encodings.delta import diffe
from warcompress.encodings.compression import gz


def encode(data_path, iframe_every=0):
    diffed = diffe.encode(data_path, iframe_every)
    ext = os.path.splitext(diffed)[1]
    bad_path = gz.encode(diffed)
    parts = os.path.splitext(bad_path)
    path = parts[0] + ext + parts[1]
    os.rename(bad_path, path)
    return path
