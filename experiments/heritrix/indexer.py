import re
import os
import sqlite3
import sys

import delta
from WARC import WARC

INDEX_SCHEMA = """
    CREATE TABLE IF NOT EXISTS record(
        job TEXT,
        path TEXT,
        offset INTEGER,
        record_id TEXT PRIMARY KEY,
        record_type TEXT,
        uri TEXT,
        date TEXT,
        digest TEXT,
        content_type TEXT
    );
    CREATE INDEX IF NOT EXISTS record_uri ON record(uri);
"""
INSERT_RECORD = """
    INSERT INTO
    record (job, path, offset,
            record_id, record_type, uri,
            date, digest, content_type)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

RID_EXISTS = 'SELECT * FROM record WHERE record_id = ?'
CT_REGEXP = re.compile(
    '^Content-Type:\s*(.*)\s*$',
    re.MULTILINE | re.IGNORECASE
)


def index(warc_dir, index_path, size_path):
    iconn = sqlite3.connect(index_path)
    icursor = iconn.cursor()
    icursor.executescript(INDEX_SCHEMA)
    iinserts = []
    sconn = sqlite3.connect(size_path)
    scursor = sconn.cursor()
    scursor.executescript(delta.SIZE_SCHEMA)
    sinserts = []
    for root, _, files in os.walk(warc_dir):
        for f in [f for f in files if f.endswith('.warc')]:
            path = os.path.join(root, f)
            for headers, content, offset in WARC(path):
                m = CT_REGEXP.search(content)
                content_type = m.group(1) if m is not None else None
                iinserts.append((
                    os.path.basename(os.path.dirname(path)),
                    path,
                    offset,
                    headers['WARC-Record-ID'],
                    headers['WARC-Type'],
                    headers.get('WARC-Target-URI', None),
                    headers['WARC-Date'],
                    headers.get('WARC-Payload-Digest', None),
                    content_type
                ))
                sinserts.append((headers['WARC-Record-ID'], len(content)))
    icursor.executemany(INSERT_RECORD, iinserts)
    iconn.commit()
    iconn.close()
    scursor.executemany(delta.INSERT_RECORD_SIZE, sinserts)
    sconn.commit()
    sconn.close()

if __name__ == '__main__':
    index(sys.argv[1], sys.argv[2])
