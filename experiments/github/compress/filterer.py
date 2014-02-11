from WARC import WARC


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


def duplicates(i):
    uris = {}
    for headers, content in i:
        if 'WARC-Payload-Digest' in headers:
            u = headers['WARC-Target-URI']
            d = headers['WARC-Payload-Digest']
            r = headers['WARC-Record-ID']
            if u in uris:
                if d in uris[u]:
                    headers = {
                        'WARC-Record-ID': r,
                        'Content-Length': '0',
                        'WARC-Date': headers['WARC-Date'],
                        'WARC-Type': 'revisit',
                        'WARC-Payload-Digest': d,
                        'WARC-Refers-To': uris[u][d],
                        'WARC-Target-URI': u,
                        'WARC-Profile': ('http://netpreserve.org/'
                                         'warc/1.0/revisit/'
                                         'identical-payload-digest')}
                    content = ''
                else:
                    uris[u][d] = r
            else:
                uris[u] = {d: r}
        yield headers, content


def localhost(warc_paths):
    keep_these = []
    for warc_path in warc_paths:
        for headers, content, _ in WARC(warc_path):
            if _record_in_local_domain(headers):
                keep_these.append(headers['WARC-Record-ID'])
        for headers, content, _ in WARC(warc_path):
            if _record_is_related(headers, keep_these):
                yield headers, content
