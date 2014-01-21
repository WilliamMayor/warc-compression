import os
import sys
import pprint
import sqlite3
from collections import defaultdict

import sql


def _churn(index_path):
    conn = sqlite3.connect(index_path)
    cursor = conn.cursor()
    stats = {}
    stats['total_gzip_size'] = cursor.execute(sql.TOTAL_SIZE, ('gzip', )).fetchone()[0]
    stats['total_targz_size'] = cursor.execute(sql.TOTAL_SIZE, ('archive.tar.gz', )).fetchone()[0]
    stats['total_zip_size'] = cursor.execute(sql.TOTAL_SIZE, ('zip', )).fetchone()[0]
    stats['total_bzip2_size'] = cursor.execute(sql.TOTAL_SIZE, ('bzip2', )).fetchone()[0]
    stats['total_tarbzip2_size'] = cursor.execute(sql.TOTAL_SIZE, ('archive.tar.bz2', )).fetchone()[0]
    stats['total_no_compression_size'] = cursor.execute(sql.TOTAL_SIZE, ('no_compression', )).fetchone()[0]
    return stats


def calculate(root_path):
    bank = defaultdict(dict)
    for l1 in ['original', 'no_duplicates', 'restructured']:
        l1_path = os.path.join(root_path, l1)
        for l2 in os.walk(l1_path).next()[1]:
            index_path = os.path.join(l1_path, l2, 'index.db')
            bank[l1][l2] = _churn(index_path)
    pprint.pprint(dict(bank))


if __name__ == '__main__':
    calculate(sys.argv[1])
