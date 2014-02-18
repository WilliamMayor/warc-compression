import time
import os
import shutil
import sys

import filterer
import indexer
import delta
import compress
from WARC import WARC


def copy_results(from_dir, to_dir):
    db_dir = os.path.join(to_dir, 'databases')
    shutil.copytree(from_dir, db_dir)
    return db_dir


def save_results(from_dir, to_dir):
    shutil.rmtree(to_dir)
    shutil.move(from_dir, to_dir)


def copy_data(from_dir, to_dir):
    warcs_dir = os.path.join(to_dir, 'warcs', 'no_delta', 'no_compression')
    count = 0
    p = os.path.join(warcs_dir, '%d.warc' % count)
    w = WARC(p)
    s = 0
    for root, _, files in os.walk(from_dir):
        filtered = filterer.duplicates(
            filterer.localhost(
                map(
                    lambda f: os.path.join(root, f),
                    filter(lambda f: f.endswith('.warc'), files))))
        for headers, content in filtered:
            s += w.add_record(headers, content)
            if s > 1024 * 1024 * 1024:
                count += 1
                p = os.path.join(warcs_dir, '%d.warc' % count)
                w = WARC(p)
    return warcs_dir


def run(data_dir, scratch_dir, results_dir):
    tick = time.time()
    job = os.path.basename(scratch_dir)
    print('Copying to scratch')
    db_dir = copy_results(results_dir, scratch_dir)
    warcs_dir = copy_data(data_dir, scratch_dir)
    index_path = os.path.join(db_dir, 'index.db')
    print('Indexing')
    indexer.index(warcs_dir, index_path)
    delta_dir = os.path.join(scratch_dir, 'warcs')
    print('Creating delta versions')
    deltas = delta.run(warcs_dir, delta_dir, db_dir, job)
    deltas.append('no_delta')
    print('Compressing')
    compress.run(deltas, delta_dir, db_dir)
    print('Copying results back to home')
    save_results(db_dir, results_dir)
    shutil.rmtree(scratch_dir)
    with open(os.path.join(results_dir, 'time_taken.txt'), 'w') as fd:
        fd.write('%d' % int(time.time() - tick))
    with open(os.path.join(results_dir, 'DONE'), 'w') as fd:
        fd.write('done\n')

if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2], sys.argv[3])
