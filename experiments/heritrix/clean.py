import os
import sqlite3
import sys

import sql


def job(index_dir, job_name):
    for root, dirs, files in os.walk(index_dir):
        for f in files:
            if f == 'index.db':
                conn = sqlite3.connect(os.path.join(root, f))
                cursor = conn.cursor()
                cursor.execute(sql.CLEAN_METADATA, (job_name,))
                cursor.execute(sql.CLEAN_RECORD, (job_name,))
                cursor.execute(sql.CLEAN_LOCATION, (job_name,))
                conn.commit()
if __name__ == '__main__':
    job(sys.argv[1], sys.argv[2])
