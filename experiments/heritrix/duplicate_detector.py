"""
python duplicate_detector.py original/warc/index.db original/warc/raw no_duplicates/warc/raw
"""
import sqlite3
import sys
import os

import utilities


DB_PATH = sys.argv[1]
ORIG_PATH = sys.argv[2]
DEST_PATH = sys.argv[3]

FIND_IDENTICAL = ('SELECT record_id'
                  ' FROM record'
                  ' WHERE digest = ?'
                  '  AND uri = ?'
                  '  AND date < ?'
                  ' ORDER BY date'
                  ' LIMIT 1')

try:
    os.makedirs(DEST_PATH)
except:
    pass

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

for root, dirs, files in os.walk(ORIG_PATH):
    for f in [f for f in files if f.endswith('.warc')]:
        print '\n', f
        old_path = os.path.join(root, f)
        new_path = os.path.join(DEST_PATH, f)
        for headers, content, offset in utilities.record_stream(old_path):
            revisit = False
            if 'WARC-Payload-Digest' in headers:
                cursor.execute(
                    FIND_IDENTICAL, (
                        headers['WARC-Payload-Digest'],
                        headers['WARC-Target-URI'],
                        headers['WARC-Date']
                    )
                )
                row = cursor.fetchone()
                if row is not None:
                    revisit = True
                    h = {
                        'WARC-Record-ID': headers['WARC-Record-ID'],
                        'Content-Length': '0',
                        'WARC-Date': headers['WARC-Date'],
                        'WARC-Type': 'revisit',
                        'WARC-Payload-Digest': headers['WARC-Payload-Digest'],
                        'WARC-Refers-To': row[0],
                        'WARC-Target-URI': headers['WARC-Target-URI'],
                        'WARC-Profile': ('http://netpreserve.org/warc/1.0/'
                                         'revisit/identical-payload-digest')
                    }
                    utilities.write_record(new_path, h, '')
            if not revisit:
                utilities.write_record(new_path, headers, content)
            sys.stdout.write('.')
            sys.stdout.flush()
