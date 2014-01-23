import sqlite3
import os

import sql


def record_sizes(from_, index_path):
    _, compression_type = os.path.split(from_)
    conn = sqlite3.connect(index_path)
    cursor = conn.cursor()
    if os.path.isdir(from_):
        for root, dirs, files in os.walk(from_):
            for f in files:
                abs_path = os.path.join(root, f)
                size = os.path.getsize(abs_path)
                cursor.execute(
                    sql.INSERT_METADATA,
                    (compression_type, abs_path, size))
    else:
        size = os.path.getsize(from_)
        cursor.execute(
            sql.INSERT_METADATA,
            (compression_type, from_, size))
    conn.commit()
    conn.close()
