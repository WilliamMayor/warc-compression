import os
import zipfile
import gzip
import bz2
import math
import random
from itertools import tee, izip


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
all = {
    'raw': raw,
    'zip': _zip,
    'gzip': _gzip,
    'bzip2': _bz2
    # use diff algorithms (diff, diffe, vcdiff, etc.)
    #   diff from previous file
    #   diff from first files
    #   diff from preceeding file (iframe)
}
