USER = 'william'
HOST = 'localhost'
JOBS_DIR = '/cs/research/fmedia/data5/wmayor/github/heritrix-3.1.1/jobs/'
DATA_DIR = '/home/wmayor/wc/data/'
DONE_PATH = '/home/wmayor/wc/done.txt'
CURRENT_PATH = '/home/wmayor/wc/current.txt'

JOBS_DIR = '/Users/william/Desktop/test_data/heritrix/jobs'
DATA_DIR = '/Users/william/Desktop/test_data/wmayor/data'
DONE_PATH = '/Users/william/Desktop/test_data/wmayor/done.txt'
CURRENT_PATH = '/Users/william/Desktop/test_data/wmayor/current.txt'

import math
import shutil
import os
import subprocess

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
    shutil.rmtree(DATA_DIR)
except:
    pass

os.makedirs(DATA_DIR)

p = subprocess.Popen(
    ['ls', JOBS_DIR],
    #['ssh', '%s@%s' % (USER, HOST), 'ls %s' % JOBS_DIR],
    stdout=subprocess.PIPE)
jobs = p.communicate()[0] or ''
jobs = jobs.strip().split('\n')
for job in [os.path.join(JOBS_DIR, j) for j in jobs]:
    if job not in done and os.path.isdir(job):
        p = subprocess.Popen(
            ['find', os.path.join(JOBS_DIR, job), '-name', '*.warc.gz', '-not', '-path', '*latest*'],
            #['ssh', '%s@%s' % (USER, HOST), 'find %s -name "*.warc.gz" -not -path "*latest*"' % os.path.join(JOBS_DIR, job)],  # NOQA
            stdout=subprocess.PIPE)
        warcs = p.communicate()[0] or ''
        warcs = warcs.strip().split('\n')
        for warc in warcs:
            subprocess.call(['cp', warc, DATA_DIR])
            #subprocess.call(['scp', '%s@%s:%s' % (USER, HOST, warc), DATA_DIR])
        with open(CURRENT_PATH, 'w') as fd:
            fd.write(os.path.join(JOBS_DIR, job))
        break

for root, dirs, files in os.walk(DATA_DIR):
    for f in files:
        subprocess.call(['gunzip', os.path.join(root, f)])

with open(DONE_PATH, 'w') as fd:
    fd.write('\n'.join(done))


def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

size = int(math.ceil(get_size(DATA_DIR) * 2 * 1.1 / (1024 * 1024 * 1024)))

instructions = """
#$ -l h_vmem=1.9G,tmem=1.9G
#$ -l fastio=1
#$ -l scr={size}G
#$ -l h_rt=1:0:0
#$ -N wmayor:warc:{job}
#$ -cwd

python job.py /home/wmayor/wc/data /scratch0/wmayor /home/wmayor/wc/results
""".format(size=size, job=job)

with open('/Users/william/Desktop/test_data/wmayor/job.sh', 'w') as fd:
    fd.write(instructions)
