import os
import random
import math
import itertools


def __window(iterable, size):
    iters = itertools.tee(iterable, size)
    for i in xrange(1, size):
        for each in iters[i:]:
            next(each, None)
    return itertools.izip(*iters)


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


def encode(data_path, n=0):
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
