import sqlite3
import os

import sql


def _insert_meta(cursor, path):
    _, compression_type = os.path.split(path)
    cursor.execute(sql.SELECT_METADATA, (compression_type, ))
    total_size, file_count = cursor.fetchone()
    try:
        for root, dirs, files in os.walk(path):
            for f in files:
                abs_path = os.path.join(root, f)
                total_size += os.path.getsize(abs_path)
                file_count += 1
    except:
        total_size += os.path.getsize(path)
        file_count += 1
    cursor.execute(
        sql.INSERT_METADATA,
        (compression_type, total_size, file_count))


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

if __name__ == '__main__':
    import sys
    SUM = 'SELECT SUM(size), COUNT(*) FROM metadata WHERE filename LIKE "%" || ?'
    DROP = 'DROP TABLE metadata'
    CREATE = 'CREATE TABLE metadata(compression_type TEXT, total_size INTEGER, file_count INTEGER)'
    for root, dirs, files in os.walk(sys.argv[1]):
        for f in [f for f in files if f == 'index.db']:
            print('  Considering ' + os.path.join(root, f))
            conn = sqlite3.connect(os.path.join(root, f))
            cursor = conn.cursor()
            cursor.execute(SUM, ('.warc.gz', ))
            gz_ts, gz_fc = cursor.fetchone()
            cursor.execute(SUM, ('.warc.zip', ))
            zip_ts, zip_fc = cursor.fetchone()
            cursor.execute(SUM, ('.warc.bz2', ))
            bz_ts, bz_fc = cursor.fetchone()
            cursor.execute(SUM, ('.warc', ))
            nc_ts, nc_fc = cursor.fetchone()
            cursor.execute(SUM, ('.tar.gz', ))
            tg_ts, tg_fc = cursor.fetchone()
            cursor.execute(SUM, ('.tar.bz2', ))
            tb_ts, tb_fc = cursor.fetchone()
            assert gz_fc == bz_fc == zip_fc
            assert tb_fc == tg_fc, '%d != %d' % (tb_fc, tg_fc)
            cursor.execute(DROP)
            cursor.execute(CREATE)
            cursor.execute(sql.INSERT_METADATA, ('no_compression', nc_ts, nc_fc))
            cursor.execute(sql.INSERT_METADATA, ('gzip', gz_ts, gz_fc))
            cursor.execute(sql.INSERT_METADATA, ('bzip2', bz_ts, bz_fc))
            cursor.execute(sql.INSERT_METADATA, ('zip', zip_ts, zip_fc))
            cursor.execute(sql.INSERT_METADATA, ('archive.tar.gz', tg_ts, tg_fc))
            cursor.execute(sql.INSERT_METADATA, ('archive.tar.bz2', tb_ts, tb_fc))
            conn.commit()
