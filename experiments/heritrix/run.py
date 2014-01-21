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
import duplicates
import structure

MAX_TIME = 11*60*60
#HERITRIX_JOBS_DIR = '/cs/research/fmedia/data5/wmayor/github/heritrix-3.1.1/jobs'  # NOQA
#DONE_PATH = '/home/wmayor/done.txt'
#DATA_DIR = '/scratch0/wmayor'
HERITRIX_JOBS_DIR = '/Users/william/Desktop/test_data/heritrix/jobs'  # NOQA
HOME_PATH = '/Users/william/Desktop/test_data/wmayor'  # NOQA
DATA_DIR = '/Users/william/Desktop/test_data/scratch0/wmayor'  # NOQA


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


def level_one(data_dir, home_dir, job):
    o_path = os.path.join(data_dir, 'original')
    o_nd_path = os.path.join(o_path, 'no_delta')
    o_nd_nc_path = os.path.join(o_nd_path, 'no_compression')
    o_index_path = os.path.join(home_dir, 'original', 'no_delta', 'index.db')
    filterer.localhost(job, o_nd_nc_path)
    indexer.index(o_nd_nc_path, o_index_path)

    nd_path = os.path.join(DATA_DIR, 'no_duplicates')
    nd_nd_path = os.path.join(nd_path, 'no_delta')
    nd_nd_nc_path = os.path.join(nd_nd_path, 'no_compression')
    nd_index_path = os.path.join(home_dir, 'no_duplicates', 'no_delta', 'index.db')
    duplicates.remove(o_nd_nc_path, nd_nd_nc_path, o_index_path)
    indexer.index(nd_nd_nc_path, nd_index_path)

    rs_path = os.path.join(DATA_DIR, 'restructured')
    rs_nd_path = os.path.join(rs_path, 'no_delta')
    rs_nd_nc_path = os.path.join(rs_nd_path, 'no_compression')
    rs_index_path = os.path.join(home_dir, 'restructured', 'no_delta', 'index.db')
    structure.by_uri(nd_nd_nc_path, rs_nd_nc_path)
    indexer.index(rs_nd_nc_path, rs_index_path)
    return ['original', 'no_duplicates', 'restructured']


def main():
    total_time = 0
    jobs_processed = 0
    done = list_done()
    for job in [j for j in list_available(HERITRIX_JOBS_DIR) if j not in done]:
        tick = time.time()
        l1 = level_one(DATA_DIR, HOME_PATH, job)
        for l in l1:
            path = os.path.join(DATA_DIR, l)
            nd_path = os.path.join(path, 'no_delta')
            nd_nc_path = os.path.join(nd_path, 'no_compression')
            index_path = os.path.join(HOME_PATH, l, 'no_delta', 'index.db')
            compress.all_the_things(nd_nc_path, nd_path)
            for d in delta.all_the_things(nd_nc_path, path, index_path):
                d_path = os.path.join(path, d)
                d_nc_path = os.path.join(d_path, 'no_compression')
                d_index = os.path.join(HOME_PATH, l, d, 'index.db')
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
    with open(os.path.join(HOME_PATH, 'done.txt'), 'w') as fd:
        fd.write('\n'.join(done))
    print('Processed %d jobs' % jobs_processed)
    print('It took %d hours' % (total_time / (60 * 60)))


if __name__ == '__main__':
    main()
