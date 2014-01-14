import shutil
import gzip
import sys
import os


def process(from_file, to_dir):
    gzr = gzip.open(from_file, 'r')
    content = gzr.read()
    gzr.close()
    warc_path = os.path.join(
        to_dir,
        'original'
        'warc',
        os.path.basename(from_file)[:-3]
    )
    with open(warc_path, 'wb') as fd:
        fd.write(content)
    gz_path = os.path.join(
        to_dir,
        'original'
        'gz',
        os.path.basename(from_file)
    )
    shutil.copyfile(from_file, gz_path)

JOBS_DIR = sys.argv[1]
DEST_DIR = sys.argv[2]

try:
    os.makedirs(os.path.join(DEST_DIR, 'original', 'gz'))
    os.makedirs(os.path.join(DEST_DIR, 'original', 'warc'))
except:
    pass

for (root, dirs, files) in os.walk(JOBS_DIR):
    for f in files:
        if f.endswith('.warc.gz'):
            print f
            process(os.path.join(root, f), DEST_DIR)
