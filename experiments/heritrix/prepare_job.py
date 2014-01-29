USER = 'wmayor'
HOST = 'wmayor'
JOBS_DIR = '/cs/research/fmedia/data5/wmayor/github/heritrix-3.1.1/jobs'
HOME_DIR = '/home/wmayor/wc'
SCRATCH_DIR = '/scratch0/wmayor'

#JOBS_DIR = '/Users/william/Desktop/test_data/heritrix/jobs'
#HOME_DIR = '/Users/william/Desktop/test_data/wmayor'
#SCRATCH_DIR = '/Users/william/Desktop/test_data/scratch0/wmayor'

import time
import math
import shutil
import os
import subprocess

import utilities

DATA_DIR = os.path.join(HOME_DIR, 'data')
DONE_PATH = os.path.join(HOME_DIR, 'done.txt')
CURRENT_PATH = os.path.join(HOME_DIR, 'current.txt')


def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def prepare():
    try:
        with open(DONE_PATH, 'r') as fd:
            done = fd.read().split('\n')
    except:
        done = []
    try:
        with open(CURRENT_PATH, 'r') as fd:
            c = fd.read().strip()
            done.append(c)
    except:
        pass
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
                fd.write('%d\n%d' % (total_time, total_size))
        os.remove(os.path.join(HOME_DIR, 'results', 'time_taken.txt'))
    except:
        pass
    try:
        shutil.rmtree(DATA_DIR)
    except:
        pass
    p = subprocess.Popen(
        #['ls', JOBS_DIR],
        ['ssh', '%s@%s' % (USER, HOST), 'ls %s' % JOBS_DIR],
        stdout=subprocess.PIPE)
    jobs = p.communicate()[0] or ''
    jobs = jobs.strip().split('\n')
    for job in jobs:
        if job not in done:
            print('Good job: ' + job)
            p = subprocess.Popen(
                #['find', os.path.join(JOBS_DIR, job), '-name', '*.warc.gz', '-not', '-path', '*latest*'],  # NOQA
                ['ssh', '%s@%s' % (USER, HOST), 'find %s -name "*.warc.gz" -not -path "*latest*"' % os.path.join(JOBS_DIR, job)],  # NOQA
                stdout=subprocess.PIPE)
            warcs = p.communicate()[0] or ''
            warcs = warcs.strip().split('\n')
            name = os.path.basename(job).replace('/', '')
            for warc in warcs:
                dest = os.path.join(DATA_DIR, name, os.path.basename(warc))
                utilities.ensure_dirs(dest)
                #subprocess.call(['cp', warc, dest])
                subprocess.call(['scp', '%s@%s:%s' % (USER, HOST, warc), dest])
            with open(CURRENT_PATH, 'w') as fd:
                fd.write(job)
            break
    else:
        return True
    for root, dirs, files in os.walk(os.path.join(DATA_DIR, name)):
        for f in files:
            subprocess.call(['gunzip', os.path.join(root, f)])
    with open(DONE_PATH, 'w') as fd:
        fd.write('\n'.join(done))
    this_size = get_size(DATA_DIR)
    safe_size = int(math.ceil(this_size * 2 * 1.1 / (1024 * 1024 * 1024)))
    hours = int(math.ceil((this_size * total_time / total_size) / (60 * 60)))
    hours = max(hours, 1)

    instructions = """
    #$ -l h_vmem=1.9G,tmem=1.9G
    #$ -l fastio=1
    #$ -l scr={size}G
    #$ -l h_rt={hours}:0:0
    #$ -N wmayor-warc-{name}
    #$ -cwd

    /share/apps/python-2.7.1/bin/python /home/wmayor/wc/code/job.py {dd}/{name} {sd}/{name} {hd}/results
    """.format(size=safe_size, hours=hours, dd=DATA_DIR,
               name=name, sd=SCRATCH_DIR, hd=HOME_DIR)

    job_path = os.path.join(HOME_DIR, 'job.sh')
    with open(job_path, 'w') as fd:
        fd.write(instructions)

    subprocess.call(['qsub', job_path])
    while subprocess.check_output(['qstat']).strip() != '':
        print('  %d: waiting' % time.time())
        time.sleep(60 * 60)
    return False

if __name__ == '__main__':
    while not prepare():
        pass
