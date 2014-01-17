import sqlite3
import os

import sql


def _calculate_size(path):
    total_size = os.path.getsize(path)
    try:
        for item in os.listdir(path):
            itempath = os.path.join(path, item)
            if os.path.isfile(itempath):
                total_size += os.path.getsize(itempath)
            elif os.path.isdir(itempath):
                total_size += _calculate_size(itempath)
    except:
        pass
    return total_size


def _insert_meta(cursor, path):
    total_size = _calculate_size(path)
    h, t = os.path.split(path)
    cursor.execute(sql.INSERT_METADATA, (h, t, total_size))
    try:
        for root, dirs, files in os.walk(path):
            for f in files:
                abs_path = os.path.join(root, f)
                rel_path = abs_path.replace(path, '')
                size = _calculate_size(abs_path)
                cursor.execute(sql.INSERT_METADATA, (path, rel_path, size))
    except:
        pass


def all_the_things(index_dir):
    print('Adding metadata')
    for root, dirs, files in os.walk(index_dir):
        for f in [f for f in files if f == 'index.db']:
            print('  Considering ' + os.path.join(root, f))
            conn = sqlite3.connect(os.path.join(root, f))
            cursor = conn.cursor()
            cursor.execute(sql.DISTINCT_LOCATIONS + ' LIMIT 1')
            path = cursor.fetchone()[0]
            nc_path = os.path.dirname(path)
            while os.path.basename(nc_path) != 'no_compression':
                nc_path = os.path.dirname(nc_path)
            data_path = os.path.dirname(nc_path)
            for item in os.listdir(data_path):
                abs_path = os.path.join(data_path, item)
                _insert_meta(cursor, abs_path)
            conn.commit()
