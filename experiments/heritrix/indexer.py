import time
import re
import os
import sqlite3
import sys

import utilities
from WARC import WARC
SCHEMA = """
    CREATE TABLE IF NOT EXISTS record(
        record_id TEXT PRIMARY KEY,
        record_type TEXT,
        uri TEXT,
        date TEXT,
        digest TEXT,
        content_type TEXT,
        content_length INTEGER
    );
    CREATE TABLE IF NOT EXISTS location(
        record_id TEXT,
        path TEXT,
        offset INTEGER,
        FOREIGN KEY(record_id) REFERENCES record(record_id),
        PRIMARY KEY(record_id, path)
    );
    CREATE INDEX IF NOT EXISTS record_uri ON record(uri);
    CREATE INDEX IF NOT EXISTS record_digest ON record(digest);
"""
INSERT_RECORD = """
    INSERT INTO record(
        record_id,
        record_type,
        uri,
        date,
        digest,
        content_type,
        content_length
    ) VALUES(?, ?, ?, ?, ?, ?, ?)
"""
INSERT_LOCATION = """
    INSERT INTO location(
        record_id,
        path,
        offset
    ) VALUES(?, ?, ?)
"""
RID_EXISTS = 'SELECT * FROM record WHERE record_id = ?'
CT_REGEXP = re.compile(
    '^Content-Type:\s*(.*)$',
    re.MULTILINE | re.IGNORECASE
)


def index(dir_, db_path):
    print 'Indexing files in', dir_
    utilities.ensure_dirs(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executescript(SCHEMA)
    conn.commit()

    tick = time.time()
    files_processed = 0
    for root, _, files in os.walk(dir_):
        for f in [f for f in files if f.endswith('.warc')]:
            path = os.path.join(root, f)
            print '  ' + path
            for headers, content, offset in WARC(path).records():
                cursor.execute(RID_EXISTS, (headers['WARC-Record-ID'], ))
                if cursor.fetchone() is not None:
                    continue
                m = CT_REGEXP.search(content)
                content_type = None
                if m:
                    content_type = m.group(1)
                cursor.execute(
                    INSERT_RECORD, (
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
                    INSERT_LOCATION, (
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
        print '    Files processed: %d' % files_processed
        print '    Time taken (seconds): %d' % int(time_taken)
        print '    Average time (seconds) per file: %d' % average_time
    except ZeroDivisionError:
        pass

if __name__ == '__main__':
    import sys
    index(sys.argv[1], sys.argv[2])
