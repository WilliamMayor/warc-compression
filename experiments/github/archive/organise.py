import os

import archive
from github.WARC import WARC

jobs_dir = '/cs/research/fmedia/data5/wmayor/github/heritrix-3.1.1/jobs'
warcs_dir = '/cs/research/fmedia/data5/wmayor/github/warcs'

for e in os.listdir(jobs_dir):
    p = os.path.join(jobs_dir, e)
    if os.path.isdir(p):
        print p
        for r in archive.filter_records(p):
            date, time = r.headers['WARC-Date'].split('T', 1)
            w = WARC(os.path.join(warcs_dir, date + ".warc"), order_by='WARC-Date')
            w.add(r)
            w.save()

print 'Done!'
