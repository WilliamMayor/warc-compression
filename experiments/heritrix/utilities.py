CRLF = '\r\n'
OPENER = 'WARC/1.0' + CRLF
OPENER_LEN = len(OPENER)
FINISHER = CRLF + CRLF
FINISHER_LEN = len(FINISHER)


def write_record(path, headers, content):
    with open(path, 'ab') as fd:
        fd.write(OPENER)
        for f, v in headers.iteritems():
            fd.write(f)
            fd.write(': ')
            fd.write(v)
            fd.write(CRLF)
        fd.write(CRLF)
        fd.write(content)
        fd.write(FINISHER)


def next_record(fd):
    opener = fd.read(OPENER_LEN)
    if opener == '':
        return None, None
    assert opener == OPENER, 'opener instead: "%s"' % opener
    headers = {}
    line = fd.readline()
    while line != CRLF:
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
    finisher = fd.read(FINISHER_LEN)
    assert finisher == FINISHER, 'finisher instead: %s' % finisher
    return headers, content


def record_stream(path):
    with open(path, 'rb') as fd:
        offset = 0
        headers, content = next_record(fd)
        while None not in [headers, content]:
            yield headers, content, offset
            offset = fd.tell()
            headers, content = next_record(fd)
