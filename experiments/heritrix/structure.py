import os

from WARC import WARC

LETTER_REPLACEMENTS = {
    '.': 'dot',
    '~': 'tilde',
    ':': 'colon',
    '/': 'slash',
    '\\': 'backslash',
    '?': 'questionmark',
    '%': 'percent',
    '*': 'asterisk',
    '|': 'pipe',
    '"': 'quote',
    '<': 'lessthan',
    '>': 'greaterthan',
    ' ': 'space'
}


def _make_path(uri):
    path = ''
    for letter in uri:
        if letter in LETTER_REPLACEMENTS:
            letter = LETTER_REPLACEMENTS[letter]
        path = os.path.join(path, letter)
    path = os.path.join(path, '{prefix}-{timestamp}-{serial}-{crawlhost}.warc')
    return path


def by_uri(from_dir, to_dir):
    print('Restructuring files')
    for root, dirs, files in os.walk(from_dir):
        for f in [f for f in files if f.endswith('.warc')]:
            print('  Considering ' + f)
            info_headers, info_content = None, None
            abs_path = os.path.join(root, f)
            for headers, content, offset in WARC(abs_path).records():
                if 'WARC-Target-URI' in headers:
                    path = _make_path(headers['WARC-Target-URI'])
                    path = os.path.join(to_dir, path)
                    w = WARC(path)
                    if not os.path.isfile(path):
                        w.add_record(info_headers, info_content)
                    w.add_record(headers, content)
                else:
                    if headers['WARC-Type'] != 'warcinfo':
                        print('    Problem! Unknown target URI')
                    else:
                        info_headers = headers
                        info_content = content
