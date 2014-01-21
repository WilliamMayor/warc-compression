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
    CREATE TABLE IF NOT EXISTS metadata(
        compression_type TEXT,
        total_size INTEGER,
        file_count INTEGER
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
INSERT_METADATA = """
    INSERT INTO metadata(
        compression_type,
        total_size,
        file_count
    ) VALUES(?, ?, ?)
"""
SELECT_METADATA = """
    SELECT total_size, file_count
    FROM metadata
    WHERE compression_type = ?
"""
FIND_PREVIOUS_RESPONSES = """
    SELECT record_id
    FROM record
    WHERE record_type = "response"
        AND uri = ?
        AND date < ?
    ORDER BY date ASC
"""
GET_LOCATION = """
    SELECT path, offset
    FROM LOCATION
    WHERE record_id = ?
"""
DISTINCT_LOCATIONS = """
    SELECT DISTINCT path
    FROM LOCATION
"""
FIND_IDENTICAL = """
    SELECT record_id
    FROM record
    WHERE digest = ?
        AND uri = ?
        AND date < ?
    ORDER BY date ASC
    LIMIT 1
"""
TOTAL_SIZE = """
    SELECT SUM(size)
    FROM metadata
    WHERE filename = ?
"""

