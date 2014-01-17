import sqlite3
import os

import sql
from WARC import WARC


def remove(from_dir, to_dir, index_path):
    conn = sqlite3.connect(index_path)
    cursor = conn.cursor()
    print('Removing duplicate records')
    for root, dirs, files in os.walk(from_dir):
        for f in [f for f in files if f.endswith('.warc')]:
            print('  Considering ' + f)
            abs_path = os.path.join(root, f)
            rel_path = abs_path.replace(from_dir, '').lstrip('/')
            new_path = os.path.join(to_dir, rel_path)
            w = WARC(new_path)
            for headers, content, offset in WARC(abs_path).records():
                if 'WARC-Payload-Digest' in headers:
                    cursor.execute(
                        sql.FIND_IDENTICAL, (
                            headers['WARC-Payload-Digest'],
                            headers['WARC-Target-URI'],
                            headers['WARC-Date']
                        )
                    )
                    row = cursor.fetchone()
                    if row is not None:
                        headers = {
                            'WARC-Record-ID': headers['WARC-Record-ID'],
                            'Content-Length': '0',
                            'WARC-Date': headers['WARC-Date'],
                            'WARC-Type': 'revisit',
                            'WARC-Payload-Digest': headers['WARC-Payload-Digest'],  # NOQA
                            'WARC-Refers-To': row[0],
                            'WARC-Target-URI': headers['WARC-Target-URI'],
                            'WARC-Profile': ('http://netpreserve.org/warc/1.0/'
                                             'revisit/identical-payload-digest')}  # NOQA
                        content = ''
                w.add_record(headers, content)
