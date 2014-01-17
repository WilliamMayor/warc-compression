import os

from WARC import WARC
from utilities import ensure_dirs


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
    print('Filtering records in ' + from_dir)
    total = 0
    kept = 0
    for root, dirs, files in os.walk(from_dir):
        for f in filter(lambda n: n.endswith('.warc.gz'), files):
            keep_these = []
            wpath = os.path.join(root, f)
            sys.stdout.write('.')
            sys.stdout.flush()
            for headers, content, _ in WARC(wpath).records():
                total += 1
                if _record_in_local_domain(headers):
                    keep_these.append(headers['WARC-Record-ID'])
            new_path = os.path.join(to_dir, f[:-3])
            ensure_dirs(new_path)
            w = WARC(new_path)
            for headers, content, _ in WARC(wpath).records():
                if _record_is_related(headers, keep_these):
                    kept += 1
                    w.add_record(headers, content)
    print('\n    Found %d records' % total)
    print('    Kept %d' % kept)

if __name__ == '__main__':
    import sys
    localhost(sys.argv[1], sys.argv[2])
