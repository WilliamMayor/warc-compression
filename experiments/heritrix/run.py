"""
python main.py

Reads through the list of available Heritrix jobs, finds one that has not been
processed and processes it.

Processing involves:
    1) Collecting the .warc files
    2) Filtering any WARC records that point outside of localhost
    3) Index the .warcs
    4) Delta and compress the .warcs
    5) Index the delted .warcs
    6) Remove duplicate WARC records
    7) Do steps 3-5 on the new .warcs
    8) Reorganise the .warcs into one file per URL
    9) Do steps 3-5 on the new .warcs


"""
import os
import time
import shutil

import compress
import filterer
import indexer
import delta
import meta

MAX_TIME = 4*60*60
#HERITRIX_JOBS_DIR = '/cs/research/fmedia/data5/wmayor/github/heritrix-3.1.1/jobs'  # NOQA
#DONE_PATH = '/home/wmayor/done.txt'
#DATA_DIR = '/scratch0/wmayor'
HERITRIX_JOBS_DIR = '/Users/william/Desktop/test_data/quick/heritrix/jobs'  # NOQA
HOME_PATH = '/Users/william/Desktop/test_data/quick/wmayor'  # NOQA
DATA_DIR = '/Users/william/Desktop/test_data/quick/scratch0/wmayor'  # NOQA


def list_done():
    try:
        with open(os.path.join(HOME_PATH, 'done.txt'), 'r') as fd:
            return [line.rstrip('\n') for line in fd]
    except:
        return []


def list_available(jobs_dir):
    available = []
    for d in os.walk(jobs_dir).next()[1]:
        # This is not fool proof, the lock file may not exist between crawls.
        if not os.path.exists(os.path.join(jobs_dir, d, 'job.log.lck')):
            available.append(os.path.join(jobs_dir, d))
    return available


def main():
    total_time = 0
    jobs_processed = 0
    done = list_done()
    nd_path = os.path.join(DATA_DIR, 'no_delta')
    nd_nc_path = os.path.join(nd_path, 'no_compression')
    nd_index = os.path.join(HOME_PATH, 'no_delta', 'index.db')
    for job in [j for j in list_available(HERITRIX_JOBS_DIR) if j not in done]:
        tick = time.time()
        filterer.localhost(job, nd_nc_path)
        compress.all_the_things(nd_nc_path, nd_path)
        indexer.index(nd_nc_path, nd_index)
        for d in delta.all_the_things(nd_nc_path, DATA_DIR, nd_index):
            d_path = os.path.join(DATA_DIR, d)
            d_nc_path = os.path.join(d_path, 'no_compression')
            d_index = os.path.join(HOME_PATH, d, 'index.db')
            compress.all_the_things(d_nc_path, d_path)
            indexer.index(d_nc_path, d_index)
        meta.all_the_things(HOME_PATH)
        done.append(job)
        shutil.rmtree(DATA_DIR)
        total_time += time.time() - tick
        jobs_processed += 1
        average_time = total_time / jobs_processed
        if total_time + 2 * average_time > MAX_TIME:
            break
    with open(os.path.join(HOME_PATH, 'done.txt'), 'r') as fd:
        fd.write('\n'.join(done))
    print('Processed %d jobs' % jobs_processed)
    print('It took %d hours' % (total_time / (60 * 60)))


if __name__ == '__main__':
    main()
