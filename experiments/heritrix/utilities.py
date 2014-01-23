import sys
import os


def ensure_dirs(path):
    try:
        os.makedirs(os.path.dirname(path))
    except:
        pass

new = True


def progress(state, end=False):
    global new
    if new:
        sys.stdout.write('  ')
    else:
        sys.stdout.write(', ')
    sys.stdout.write(state)
    new = False
    if end:
        sys.stdout.write('\n')
        new = True
    sys.stdout.flush()
