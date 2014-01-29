"""
python main.py

Reads through the list of available Heritrix jobs, finds one that has not been
processed and processes it.

Processing involves:
    1) Collecting the .warc files
    2) Filtering any WARC records that point outside of localhost
    3) Index the .warcs
    4) Delta and compress the .warcs
    5) Index the delta-ed .warcs
    6) Remove duplicate WARC records
    7) Do steps 3-5 on the new .warcs
    8) Reorganise the .warcs into one file per URL
    9) Do steps 3-5 on the new .warcs


"""
import os
import shutil
import sys
import time

import compress
import filterer
import indexer
import delta
import meta
import duplicates
import structure


def run(job_dir, working_dir, store_dir):
    print('Running job ' + job_dir)
    tick = time.time()
    filtered_dir = os.path.join(working_dir, 'original',
                                'no_delta', 'no_compression')
    filterer.localhost(job_dir, filtered_dir)
    mip = os.path.join(store_dir, 'original', 'no_delta', 'index.db')
    indexer.index(filtered_dir, mip)
    meta.record_sizes(filtered_dir, mip)
    compress.all_the_things(filtered_dir, working_dir, mip)
    delta.all_the_things(
        filtered_dir,
        working_dir,
        os.path.join(store_dir, 'original'),
        mip)

    no_dup_dir = os.path.join(working_dir, 'no_duplicates',
                              'no_delta', 'no_compression')
    duplicates.remove(filtered_dir, no_dup_dir, mip)
    shutil.rmtree(filtered_dir)
    ndip = os.path.join(store_dir, 'no_duplicates', 'no_delta', 'index.db')
    indexer.index(no_dup_dir, ndip)
    meta.record_sizes(no_dup_dir, ndip)
    compress.all_the_things(no_dup_dir, working_dir, ndip)
    delta.all_the_things(
        no_dup_dir,
        working_dir,
        os.path.join(store_dir, 'no_duplicates'),
        ndip)

    restruct_dir = os.path.join(working_dir, 'restructured',
                                'no_delta', 'no_compression')
    structure.by_uri(no_dup_dir, restruct_dir)
    shutil.rmtree(no_dup_dir)
    rsip = os.path.join(store_dir, 'restructured', 'no_delta', 'index.db')
    indexer.index(restruct_dir, rsip)
    meta.record_sizes(restruct_dir, rsip)
    compress.all_the_things(restruct_dir, working_dir, rsip)
    delta.all_the_things(
        restruct_dir,
        working_dir,
        os.path.join(store_dir, 'restructured'),
        rsip)

    shutil.rmtree(working_dir)
    tock = time.time()
    with open(os.path.join(store_dir, 'time_taken.txt'), 'w') as fd:
        fd.write(str(int(tock - tick)))
    print('Done')


if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2], sys.argv[3])
