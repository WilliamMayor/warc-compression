import os
import bz2
import gzip


class WARC:

    CRLF = '\r\n'
    OPENER = 'WARC/1.0' + CRLF
    OPENER_LEN = len(OPENER)
    FINISHER = CRLF + CRLF
    FINISHER_LEN = len(FINISHER)

    def __init__(self, path):
        self.path = path
        self.gzipped = path.endswith('.gz')
        self.bzipped = path.endswith('.bz2')

    def _open(self, mode):
        if self.gzipped:
            return gzip.open(self.path, mode)
        elif self.bzipped:
            return bz2.BZ2File(self.path, mode)
        else:
            return open(self.path, mode)

    def add_record(self, headers, content):
        try:
            os.makedirs(os.path.dirname(self.path))
        except:
            pass
        fd = self._open('ab')
        start = fd.tell()
        fd.write(WARC.OPENER)
        for f, v in headers.iteritems():
            fd.write(f)
            fd.write(': ')
            fd.write(v)
            fd.write(WARC.CRLF)
        fd.write(WARC.CRLF)
        fd.write(content)
        fd.write(WARC.FINISHER)
        size = fd.tell() - start
        fd.close()
        return size

    def get_record(self, seek):
        fd = self._open('rb')
        fd.seek(seek)
        headers, content = self._next_record(fd)
        fd.close()
        return headers, content

    def __iter__(self):
        fd = self._open('rb')
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
