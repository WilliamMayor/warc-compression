"""
Filter WARC files, keeping only those files that contain a record with a matching header.
e.g. to keep all WARC files that contain at least one reference to example.com:
    python filter.py FROM_DIR TO_DIR 'WARC-Target-URI:.*example.com.*'
"""
import shutil
import re
import os
import sys

import utilities


def should_keep(path, criteria):
    for headers, content, offset in utilities.record_stream(path):
        for h, m in criteria.iteritems():
            if h in headers:
                if m.match(headers[h]):
                    return True
    return False


def parse_criteria(raw):
    criteria = {}
    for part in raw:
        field, regexp = part.split(':', 1)
        criteria[field] = re.compile(regexp)
    return criteria


from_path = sys.argv[1]
to_path = sys.argv[2]
criteria = parse_criteria(sys.argv[3:])

for root, dirs, files in os.walk(from_path):
    for f in [f for f in files if f.endswith('.warc')]:
        warc_path = os.path.join(root, f)
        if should_keep(warc_path, criteria):
            new_path = os.path.join(to_path, f)
            shutil.copyfile(warc_path, new_path)
