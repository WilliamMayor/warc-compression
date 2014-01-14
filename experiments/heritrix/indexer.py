"""
python indexer.py ./original/warc
"""
import time
import re
import os
import sqlite3
import sys

import utilities

BASE_PATH = sys.argv[1]
DB_PATH = os.path.join(BASE_PATH, 'index.db')

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
CREATE TABLE IF NOT EXISTS concurrent(
    `from` TEXT,
    `to` TEXT,
    FOREIGN KEY(`from`) REFERENCES record(record_id),
    FOREIGN KEY(`to`) REFERENCES record(record_id)
);
CREATE INDEX IF NOT EXISTS record_uri ON record(uri);
CREATE INDEX IF NOT EXISTS record_digest ON record(digest);
CREATE INDEX IF NOT EXISTS concurrent_from ON concurrent(`from`);
CREATE INDEX IF NOT EXISTS concurrent_to ON concurrent(`to`);
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
INSERT_CONCURRENT = """
    INSERT INTO concurrent(
        `from`,
        `to`
    ) VALUES(?, ?)
"""
RID_EXISTS = 'SELECT * FROM record WHERE record_id = ?'
CT_REGEXP = re.compile(
    '^Content-Type:\s*(.*)$',
    re.MULTILINE | re.IGNORECASE
)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.executescript(SCHEMA)
conn.commit()

time_taken = 0
files_processed = 0
for root, dirs, files in os.walk(BASE_PATH):
    for f in [f for f in files if f.endswith('.warc')]:
        tick = time.time()
        print f
        path = os.path.join(root, f)
        for headers, content, offset in utilities.record_stream(path):
            cursor.execute(RID_EXISTS, (headers['WARC-Record-ID'], ))
            if cursor.fetchone() is not None:
                print 'done'
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
            if 'WARC-Concurrent-To' in headers:
                ids = headers['WARC-Concurrent-To']
                if isinstance(ids, basestring):
                    ids = [ids]
                for i in ids:
                    cursor.execute(
                        INSERT_CONCURRENT, (
                            headers['WARC-Record-ID'],
                            i
                        )
                    )
            conn.commit()
            sys.stdout.write('.')
            sys.stdout.flush()
        time_taken += time.time() - tick
        files_processed += 1
        try:
            average_time = int(time_taken / files_processed)
            print '\nFiles processed: %d' % files_processed
            print 'Time taken (seconds): %d' % int(time_taken)
            print 'Average time (seconds) per file: %d' % average_time
        except ZeroDivisionError:
            pass
