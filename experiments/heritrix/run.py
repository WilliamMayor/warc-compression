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
MAX_TIME = 4*60*60
HERITRIX_JOBS_DIR = '/cs/research//fmedia/data5/wmayor/github/heritrix-3.1.1/jobs'  # NOQA
DONE_PATH = '/home/wmayor/done.txt'
DATA_DIR = '/scratch0'

DELTA_FUNCS = [
    ('no_delta', raw),
    ('bsdiff@first', lambda x: bsdiff(x, first=True)),
    ('bsdiff@previous', lambda x: bsdiff(x, previous=True)),
    ('bsdiff@2', lambda x: bsdiff(x, iframe=2)),
    ('bsdiff@5', lambda x: bsdiff(x, iframe=5)),
    ('bsdiff@10', lambda x: bsdiff(x, iframe=10)),
    ('vcdiff@first', lambda x: vcdiff(x, first=True)),
    ('vcdiff@previous', lambda x: vcdiff(x, previous=True)),
    ('vcdiff@2', lambda x: vcdiff(x, iframe=2)),
    ('vcdiff@5', lambda x: vcdiff(x, iframe=5)),
    ('vcdiff@10', lambda x: vcdiff(x, iframe=10)),
    ('diffe@first', lambda x: diffe(x, first=True)),
    ('diffe@previous', lambda x: diffe(x, previous=True)),
    ('diffe@2', lambda x: diffe(x, iframe=2)),
    ('diffe@5', lambda x: diffe(x, iframe=5)),
    ('diffe@10', lambda x: diffe(x, iframe=10))
]


def list_available(jobs_dir):
    available = []
    for d in os.walk(jobs_dir).next()[1]:
        # This is not fool proof, the lock file may not exist between crawls.
        if not os.path.exists(os.path.join(jobs_dir, d, 'job.log.lck')):
            available.append(os.path.join(jobs_dir, d))
    return available


def main():
    with open(DONE_PATH, 'r') as fd:
        done = [line.rstrip('\n') for line in fd]
    raw_path = os.path.join(
        DATA_DIR,
        'no_delta',
        'no_compression',
        'warcs'
    )
    raw_index = os.path.join(
        DATA_DIR,
        'no_delta',
        'no_compression',
        'index.db'
    )
    for available in list_available(HERITRIX_JOBS_DIR):
        if available not in done:
            for warc in list_warcs(available):
                filter_and_copy_records(
                    warc,
                    os.path.join(
                        raw_path,
                        os.path.basename(warc)))
            warcs = list_warcs(raw_path)
            for name, delta_func in DELTA_FUNCS:
                to_dir = os.path.join(
                    DATA_DIR,
                    name,
                    'no_compression',
                    'warcs')
                index_path = os.path.join(
                    DATA_DIR,
                    name,
                    'index.db')
                delta_func(warcs, to_dir, raw_index)
                index(to_dir, index_path)
                compress(to_dir)
            for warc in list_warcs(raw_path):
                index(warc, raw_index)
            compress(raw_path, raw_index)


if __name__ == '__main__':
    main()
