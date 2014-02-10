import subprocess
import os

import sge_prepare

check_file = os.path.join(sge_prepare.HOME_DIR, 'results', 'DONE')

if os.path.isfile(check_file):
    os.remove(check_file)
    sge_prepare.prepare()
    subprocess.call([
        '/Users/william/bin/qsub',
        os.path.join(sge_prepare.HOME_DIR, 'job.sh')])
