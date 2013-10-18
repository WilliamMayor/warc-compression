import os
import tempfile

RECORD_SEPARATOR = '\n---warcompress test---\n'


def __pair(files, iframe_every):
    if iframe_every == 0 or iframe_every >= len(files):
        return [(files[0], files[1:])]
    pairs = []
    j = max(iframe_every, 2)
    for i in xrange(0, len(files), iframe_every):
        pairs.append((files[i], files[i+1:i+j]))
    return pairs


def __deltas(algorithm, pairs):
    for (base_text, steps) in pairs:
        base_temp = tempfile.NamedTemporaryFile()
        base_temp.write(base_text)
        base_temp.flush()
        for step_text in steps:
            step_temp = tempfile.NamedTemporaryFile()
            step_temp.write(step_text)
            step_temp.flush()
            yield algorithm(base_temp.name, step_temp.name)
            step_temp.close()
        base_temp.close()


def diff(ext, algorithm, data_path, iframe_every):
    path = '.'.join([
        os.path.splitext(data_path)[0],
        'i%d' % iframe_every,
        ext
    ])
    with open(data_path, 'r') as fd:
        raw_text = fd.read()
    files = raw_text.split(RECORD_SEPARATOR)
    with open(path, 'w') as fd:
        pairs = __pair(files, iframe_every)
        fd.write(files[0])
        i = 1
        for delta in __deltas(algorithm, pairs):
            fd.write(RECORD_SEPARATOR)
            if iframe_every not in [0, 1] and i % iframe_every == 0:
                fd.write(files[i])
            else:
                fd.write(delta)
            i += 1
    return path
