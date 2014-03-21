import os
import bz2
import gzip


class Record:

    def __init__(self, headers, content):
        self.headers = headers
        self.content = content

    def __repr__(self):
        return '<Record %s>' % self.headers['WARC-Record-ID']


class WARC:

    CRLF = '\r\n'
    OPENER = 'WARC/1.0' + CRLF
    OPENER_LEN = len(OPENER)
    FINISHER = CRLF + CRLF
    FINISHER_LEN = len(FINISHER)

    def __init__(self, path, order_by=None):
        self.path = path
        self.order_by = order_by
        self.records = []
        self._read = False

    def _open(self, mode):
        if self.path.endswith('.gz'):
            return gzip.open(self.path, mode)
        elif self.path.endswith('.bz2'):
            return bz2.BZ2File(self.path, mode)
        else:
            return open(self.path, mode)

    def _compare_factory(self, key):
        def cmp_(x, y):
            if x.headers['WARC-Type'].lower() == 'warcinfo':
                return -1
            if y.headers['WARC-Type'].lower() == 'warcinfo':
                return 1
            if x.headers['WARC-Record-ID'] == y.headers['WARC-Record-ID']:
                return 0
            try:
                x = x.headers[key]
                try:
                    y = y.headers[key]
                    return cmp(x, y)
                except:
                    return -1
            except:
                return 1
        return cmp_

    def save(self):
        if not self._read:
            self._read_from_file()
            self._read = True
        try:
            os.makedirs(os.path.dirname(self.path))
        except:
            pass
        fd = self._open('wb')
        _records = self.records
        if self.order_by is not None:
            _records = sorted(_records, cmp=self._compare_factory(self.order_by))
        for r in _records:
            fd.write(WARC.OPENER)
            for f, v in r.headers.iteritems():
                fd.write(f)
                fd.write(': ')
                fd.write(v)
                fd.write(WARC.CRLF)
            fd.write(WARC.CRLF)
            fd.write(r.content)
            fd.write(WARC.FINISHER)
        size = fd.tell()
        fd.close()
        return size

    def get_record(self, seek):
        fd = self._open('rb')
        fd.seek(seek)
        record = self._next_record(fd)
        fd.close()
        return record

    def __iter__(self):
        if not self._read:
            self._read_from_file()
            self._read = True
        for r in self.records:
            yield r


    def _read_from_file(self):
        try:
            fd = self._open('rb')
            record = self._next_record(fd)
            while record is not None:
                self.add(record)
                record = self._next_record(fd)
            fd.close()
        except IOError:
            pass

    def _next_record(self, fd):
        opener = fd.read(WARC.OPENER_LEN)
        if opener == '':
            return None
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
        assert finisher == WARC.FINISHER, 'in %s, finisher instead: %s, at %d' % (fd.name, finisher, fd.tell() - WARC.FINISHER_LEN)
        return Record(headers, content)

    def add(self, record):
        self.records.append(record)

