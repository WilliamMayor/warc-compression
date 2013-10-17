import os
import zipfile
import gzip
import bz2
import math
import random
import subprocess
import tempfile

from itertools import tee, izip

RECORD_SEPARATOR = '\n---warcompress test---\n'


def raw(data_path):
    return data_path


def _zip(data_path):
    path = os.path.splitext(data_path)[0] + '.zip'
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.write(data_path)
    return path


def _gzip(data_path):
    path = os.path.splitext(data_path)[0] + '.gz'
    f_in = open(data_path, 'rb')
    f_out = gzip.open(path, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    return path


def _bz2(data_path):
    path = os.path.splitext(data_path)[0] + '.bz2'
    f_in = open(data_path, 'rb')
    f_out = bz2.BZ2File(path, 'w')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    return path


def __diffe(source, target):
    with open(os.devnull, 'wb') as devnull:
        return subprocess.Popen(
            ['diff', '--ed', source, target],
            stdout=subprocess.PIPE,
            stderr=devnull
        ).communicate()[0]


def __vcdiff(source, target):
    with open(os.devnull, 'wb') as devnull:
        return subprocess.Popen(
            ['vcdiff', 'delta', '-dictionary', source, '-target', target],
            stdout=subprocess.PIPE,
            stderr=devnull
        ).communicate()[0]


def __bsdiff(source, target):
    patch = tempfile.NamedTemporaryFile()
    with open(os.devnull, 'wb') as devnull:
        subprocess.call(
            ['bsdiff', source, target, patch.name],
            stdout=devnull,
            stderr=devnull
        )
    text = patch.read()
    patch.close()
    return text


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


def __diff(ext, algorithm, data_path, iframe_every):
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


def diffe(data_path, iframe_every):
    return __diff('diffe', __diffe, data_path, iframe_every)


def vcdiff(data_path, iframe_every):
    return __diff('vcdiff', __vcdiff, data_path, iframe_every)


def bsdiff(data_path, iframe_every):
    return __diff('bsdiff', __bsdiff, data_path, iframe_every)


def diffe_gzip(data_path, iframe_every):
    diffed = diffe(data_path, iframe_every)
    ext = os.path.splitext(diffed)[1]
    bad_path = _gzip(diffed)
    parts = os.path.splitext(bad_path)
    path = parts[0] + ext + parts[1]
    os.rename(bad_path, path)
    return path


def __window(iterable, size):
    iters = tee(iterable, size)
    for i in xrange(1, size):
        for each in iters[i:]:
            next(each, None)
    return izip(*iters)


def __tree_add(tree, w):
    head = tree
    for c in w:
        if c not in head:
            head[c] = {'total': 1}
        else:
            head[c]['total'] = head[c]['total'] + 1
        head = head[c]


def __build_tree(text, depth):
    tree = {}
    tree['total'] = len(text)
    for w in __window(text, depth):
        __tree_add(tree, w)
    for i in xrange(len(text) - depth + 1, len(text)):
        __tree_add(tree, text[i:])
    return tree


def __calculate_entropy(tree, n):
    if len(tree.keys()) == 1:
        p = float(tree['total']) / n
        return p * -math.log(p, 2)
    else:
        entropy = 0
        for c in tree:
            if c != 'total':
                entropy += __calculate_entropy(tree[c], tree['total'])
        p = float(tree['total']) / n
        return p * entropy


def __random_character(seed_text, tree):
    for c in seed_text:
        tree = tree[c]
    possibles = ''
    for char, details in tree.iteritems():
        if char != 'total':
            possibles += char*details['total']
    return random.choice(possibles)


def __random_text(tree, tree_depth, length):
    text = ''
    for count in xrange(0, length):
        if tree_depth == 1:
            seed_text = ''
        else:
            seed_text = text[-(tree_depth-1):]
        text += __random_character(seed_text, tree)
    return text


def n_order_optimal(data_path, n):
    path = os.path.splitext(data_path)[0] + '.%do' % n
    with open(data_path, 'r') as fd:
        text = fd.read()
        size = len(text)
    if n == 0:
        text = ''.join(set(text))
        n = 1
    tree = __build_tree(text, n)
    entropy = __calculate_entropy(tree, len(text))
    optimal = int(entropy * size / 8)
    with open(path, 'w') as fd:
        text = None
        while text is None:
            try:
                text = __random_text(tree, n, optimal)
            except IndexError:
                # Could not generate random text,
                # ran out of possibilities.'
                # Trying again.
                pass
        fd.write(text)
    return path

compression = {
    'raw': raw,
    'zip': _zip,
    'gzip': _gzip,
    'bzip2': _bz2
}
diff = {
    'diffe': diffe,
    'diffe_gzip': diffe_gzip,
    'vcdiff': vcdiff,
    'bsdiff': bsdiff
    # use diff algorithms (diff, diffe, vcdiff, etc.)
    #   diff from previous file
    #   diff from first files
    #   diff from preceeding file (iframe)
}
