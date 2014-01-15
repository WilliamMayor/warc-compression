import gzip

import utilities


class WARC:

    CRLF = '\r\n'
    OPENER = 'WARC/1.0' + CRLF
    OPENER_LEN = len(OPENER)
    FINISHER = CRLF + CRLF
    FINISHER_LEN = len(FINISHER)

    def __init__(self, path):
        self.path = path
        self.gzipped = path.endswith('.gz')

    def add_record(self, headers, content):
        utilities.ensure_dirs(self.path)
        if self.gzipped:
            fd = gzip.open(self.path, 'ab')
        else:
            fd = open(self.path, 'ab')
        fd.write(WARC.OPENER)
        for f, v in headers.iteritems():
            fd.write(f)
            fd.write(': ')
            fd.write(v)
            fd.write(WARC.CRLF)
        fd.write(WARC.CRLF)
        fd.write(content)
        fd.write(WARC.FINISHER)
        fd.close()

    def get_record(self, seek):
        with open(self.path, 'rb') as fd:
            fd.seek(seek)
            return self._next_record(fd)

    def records(self):
        if self.gzipped:
            fd = gzip.open(self.path, 'rb')
        else:
            fd = open(self.path, 'rb')
        offset = 0
        headers, content = self._next_record(fd)
        while None not in [headers, content]:
            yield headers, content, offset
            offset = fd.tell()
            headers, content = self._next_record(fd)
        fd.close()

    def _next_record(self, fd):
        opener = fd.read(WARC.OPENER_LEN)
        if opener == '':
            return None, None
        assert opener == WARC.OPENER, 'opener instead: "%s"' % opener
        headers = {}
        line = fd.readline()
        while line != WARC.CRLF:
            field, _, value = line.partition(':')
            value = value.strip()
            if field in headers:
                try:
                    headers[field].append(value)
                except:
                    headers[field] = [headers[field], value]
            else:
                headers[field] = value
            line = fd.readline()
        content_length = int(headers['Content-Length'])
        content = fd.read(content_length)
        finisher = fd.read(WARC.FINISHER_LEN)
        assert finisher == WARC.FINISHER, 'finisher instead: %s' % finisher
        return headers, content
