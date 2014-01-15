import os


def ensure_dirs(path):
    try:
        os.makedirs(os.path.dirname(path))
    except:
        pass
