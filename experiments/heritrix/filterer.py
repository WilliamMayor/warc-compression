import sys
import os

from WARC import WARC
from utilities import ensure_dirs, progress


def _record_in_local_domain(headers):
    return all([
        headers['WARC-Type'] == 'response',
        'localhost:' in headers.get('WARC-Target-URI', '')
    ])


def _record_is_related(headers, allowed):
    return any([
        headers['WARC-Record-ID'] in allowed,
        headers.get('WARC-Concurrent-To', None) in allowed,
        headers.get('WARC-Refers-To', None) in allowed,
        headers['WARC-Type'] == 'warcinfo'
    ])


def localhost(from_dir, to_dir):
    progress('Filtering records', end=True)
    for root, dirs, files in os.walk(from_dir):
        if 'latest' in root:
            continue
        for f in filter(lambda n: n.endswith('.warc.gz') or n.endswith('.warc'), files):
            keep_these = []
            wpath = os.path.join(root, f)
            for headers, content, _ in WARC(wpath).records():
                if _record_in_local_domain(headers):
                    keep_these.append(headers['WARC-Record-ID'])
            new_path = os.path.join(to_dir, f.replace('.gz', ''))
            ensure_dirs(new_path)
            w = WARC(new_path)
            for headers, content, _ in WARC(wpath).records():
                if _record_is_related(headers, keep_these):
                    w.add_record(headers, content)

if __name__ == '__main__':
    localhost(sys.argv[1], sys.argv[2])
