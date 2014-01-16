import time
import re
import os
import sqlite3

import utilities
import sql
from WARC import WARC

RID_EXISTS = 'SELECT * FROM record WHERE record_id = ?'
CT_REGEXP = re.compile(
    '^Content-Type:\s*(.*)$',
    re.MULTILINE | re.IGNORECASE
)


def index(dir_, db_path):
    print('Indexing files in', dir_)
    utilities.ensure_dirs(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executescript(sql.SCHEMA)
    conn.commit()

    tick = time.time()
    files_processed = 0
    for root, _, files in os.walk(dir_):
        for f in [f for f in files if f.endswith('.warc')]:
            path = os.path.join(root, f)
            print('  ' + f)
            for headers, content, offset in WARC(path).records():
                cursor.execute(RID_EXISTS, (headers['WARC-Record-ID'], ))
                if cursor.fetchone() is not None:
                    continue
                m = CT_REGEXP.search(content)
                content_type = None
                if m:
                    content_type = m.group(1)
                cursor.execute(
                    sql.INSERT_RECORD, (
                        headers['WARC-Record-ID'],
                        headers['WARC-Type'],
                        headers.get('WARC-Target-URI', None),
                        headers['WARC-Date'],
                        headers.get('WARC-Payload-Digest', None),
                        content_type,
                        int(headers['Content-Length'])
                    )
                )
                cursor.execute(
                    sql.INSERT_LOCATION, (
                        headers['WARC-Record-ID'],
                        path,
                        offset
                    )
                )
                conn.commit()
            files_processed += 1
    try:
        time_taken = time.time() - tick
        average_time = int(time_taken / files_processed)
        print('    Files processed: %d' % files_processed)
        print('    Time taken (seconds): %d' % int(time_taken))
        print('    Average time (seconds) per file: %d' % average_time)
    except ZeroDivisionError:
        pass

if __name__ == '__main__':
    import sys
    index(sys.argv[1], sys.argv[2])
