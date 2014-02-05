USER = 'wmayor'
HOST = 'wmayor'
JOBS_DIR = '/cs/research/fmedia/data5/wmayor/github/heritrix-3.1.1/jobs'
HOME_DIR = '/home/wmayor/wc'
SCRATCH_DIR = '/scratch0/wmayor'
PYTHON_PATH = '/share/apps/python-2.7.1/bin/python'
CODE_PATH = '/home/wmayor/wc/code'

JOBS_DIR = '/Users/william/Desktop/test_data/heritrix/jobs'
HOME_DIR = '/Users/william/Desktop/test_data/home'
SCRATCH_DIR = '/Users/william/Desktop/test_data/scratch0'
PYTHON_PATH = 'python'
CODE_PATH = '/Users/william/Projects/warc-compression/experiments/heritrix'

import math
import shutil
import os
import subprocess

DATA_DIR = os.path.join(HOME_DIR, 'data')
DONE_PATH = os.path.join(HOME_DIR, 'done.txt')
CURRENT_PATH = os.path.join(HOME_DIR, 'current.txt')

JOB_TEMPLATE = """
#$ -l h_vmem=1.9G
#$ -l tmem=1.9G
#$ -l fastio=1
#$ -l scr={size}G
#$ -l h_rt={hours}:0:0
#$ -N wmayor-warc-{name}
#$ -cwd

{python} {job} {data} {scratch} {results}
"""


def ensure_dirs(path):
    try:
        os.makedirs(os.path.dirname(path))
    except:
        pass


def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def get_done():
    try:
        with open(DONE_PATH, 'r') as fd:
            done = fd.read().strip().split('\n')
    except:
        done = []
    try:
        with open(CURRENT_PATH, 'r') as fd:
            done.append(fd.read().strip())
    except:
        pass
    return done


def save_done(done, job):
    with open(CURRENT_PATH, 'w') as fd:
                fd.write(job)
    with open(DONE_PATH, 'w') as fd:
        fd.write('\n'.join(done))


def get_stats():
    try:
        with open(os.path.join(HOME_DIR, 'stats.txt'), 'r') as fd:
            total_time = float(fd.readline().strip())
            total_size = float(fd.readline().strip())
    except:
        total_time = 0
        total_size = 1
    try:
        tt_path = os.path.join(HOME_DIR, 'results', 'time_taken.txt')
        with open(tt_path, 'r') as fd:
            last_time_taken = int(fd.read())
            last_size = get_size(DATA_DIR)
            total_time += last_time_taken
            total_size += last_size
            with open(os.path.join(HOME_DIR, 'stats.txt'), 'w') as fd:
                fd.write('%d\n%d\n' % (total_time, total_size))
    except:
        pass
    return total_time, total_size


def clean_last():
    for old in [DATA_DIR, os.path.join(HOME_DIR, 'time_taken.txt')]:
        try:
            shutil.rmtree(old)
        except:
            pass


def get_jobs():
    p = subprocess.Popen(
        ['ls', JOBS_DIR],
        #['ssh', '%s@%s' % (USER, HOST), 'ls %s' % JOBS_DIR],
        stdout=subprocess.PIPE)
    jobs = p.communicate()[0] or ''
    return jobs.strip().split('\n')


def get_warcs(path):
    p = subprocess.Popen(
        ['find',
         path,
         '-name', '*.warc.gz',
         '-not', '-path', '*latest*'],
        #['ssh',
        # '%s@%s' % (USER, HOST),
        # 'find %s -name "*.warc.gz" -not -path "*latest*"' % path],
        stdout=subprocess.PIPE)
    warcs = p.communicate()[0] or ''
    warcs = warcs.strip().split('\n')
    return filter(lambda w: w != '', warcs)


def copy_warcs(warcs, job):
    print('  Copying %d WARCs' % len(warcs))
    for warc in warcs:
        dest = os.path.join(DATA_DIR, job, os.path.basename(warc))
        ensure_dirs(dest)
        subprocess.call(['cp', warc, dest])
        #subprocess.call([
        #    'scp',
        #    '%s@%s:%s' % (USER, HOST, warc), dest])


def gunzip():
    print('  gunzipping')
    for root, dirs, files in os.walk(DATA_DIR):
        for f in filter(lambda f: f.endswith('.gz'), files):
            subprocess.call(['gunzip', os.path.join(root, f)])


def prepare():
    done = get_done()
    total_time, total_size = get_stats()
    clean_last()

    for job in get_jobs():
        if job not in done:
            print('Found job: ' + job)
            path = os.path.join(JOBS_DIR, job)
            warcs = get_warcs(path)
            if len(warcs) == 0:
                print('  no warcs')
                done.append(job)
                continue
            copy_warcs(warcs, job)
            break
    else:
        print('All done')
        return
    gunzip()

    save_done(done, job)

    job_size = get_size(DATA_DIR)
    secs_per_byte = float(total_time) / total_size
    hours = math.ceil((job_size * secs_per_byte) / (60 * 60))
    hours = max(hours, 1)

    print('Estimated seconds per megabyte: %f' % (secs_per_byte * 1024 * 1024))
    print('Current bytes: %d' % job_size)
    print('Estimated time: %dhours' % hours)

    results_size = get_size(os.path.join(HOME_DIR, 'results'))
    safe_size = math.ceil(
        (job_size * 11 + results_size * 1.1) * 1.1 / (1024 * 1024 * 1024))

    job_path = os.path.join(HOME_DIR, 'job.sh')
    with open(job_path, 'w') as fd:
        fd.write(JOB_TEMPLATE.format(
            size=int(safe_size),
            hours=int(hours),
            name=job,
            python=PYTHON_PATH,
            job=os.path.join(CODE_PATH, 'job.py'),
            data=DATA_DIR,
            scratch=os.path.join(SCRATCH_DIR, job),
            results=os.path.join(HOME_DIR, 'results')))

if __name__ == '__main__':
    prepare()
