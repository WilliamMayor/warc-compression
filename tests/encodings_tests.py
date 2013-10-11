from nose.tools import assert_equal
from warcompress.experiments.encodings import __build_tree, __calculate_entropy, __random_generate


def setup():
    print "SETUP!"


def teardown():
    print "TEAR DOWN!"


def test_build_tree_1_order():
    text = 'abcdd'
    got = __build_tree(text, 1)
    expected = {
        'a': {'total': 1},
        'b': {'total': 1},
        'c': {'total': 1},
        'd': {'total': 2},
        'total': 5
    }
    assert_equal(expected, got)


def test_calculate_entropy_1_order():
    text = 'abcdd'
    tree = __build_tree(text, 1)
    got = __calculate_entropy(tree, len(text))
    expected = 1.9219280948873623
    assert_equal(expected, got)


def test_build_tree_2_order():
    text = 'abcdd'
    got = __build_tree(text, 2)
    expected = {
        'a': {
            'b': {'total': 1},
            'total': 1
        },
        'b': {
            'c': {'total': 1},
            'total': 1
        },
        'c': {
            'd': {'total': 1},
            'total': 1
        },
        'd': {
            'd': {'total': 1},
            'total': 2
        },
        'total': 5
    }
    assert_equal(expected, got)


def test_calculate_entropy_2_order():
    text = 'abcdd'
    tree = __build_tree(text, 2)
    got = __calculate_entropy(tree, len(text))
    expected = 0.2
    assert_equal(expected, got)


def test_build_tree_3_order():
    text = 'abcdd'
    got = __build_tree(text, 3)
    expected = {
        'a': {
            'b': {
                'c': {'total': 1},
                'total': 1
            },
            'total': 1
        },
        'b': {
            'c': {
                'd': {'total': 1},
                'total': 1
            },
            'total': 1
        },
        'c': {
            'd': {
                'd': {'total': 1},
                'total': 1
            },
            'total': 1
        },
        'd': {
            'd': {'total': 1},
            'total': 2
        },
        'total': 5
    }
    assert_equal(expected, got)


def test_random_generate():
    text = 'abcdd'
    for i in xrange(1, 3):
        tree = __build_tree(text, i)
        random_text = ''
        for _ in xrange(10):
            if i == 1:
                seed_text = ''
            else:
                seed_text = text[-(i-1):]
            random_text += __random_generate(seed_text, tree)
        print random_text
