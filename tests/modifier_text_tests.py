import string

from nose.tools import assert_equal, assert_in
from warcompress.modifiers.text import (
    TextDelete,
    TextInsert,
    TextSubstitute
)


def test_delete_all():
    text = 'abc'
    modifier = TextDelete().modify(text)
    for deleted in modifier:
        pass
    assert_equal(0, len(deleted))


def test_delete_blocksize_default():
    text = 100 * 'a'
    modifier = TextDelete().modify(text)
    for deleted in modifier:
        pass
    assert_equal(len(text) - 20, len(deleted))


def test_delete_blocksize_2():
    text = 100 * 'a'
    modifier = TextDelete(blocksize=2).modify(text)
    for deleted in modifier:
        pass
    assert_equal(len(text) - 20*2, len(deleted))


def test_delete_count_default():
    text = 'abc' * 100
    modifier = TextDelete().modify(text)
    assert_equal(21, len(list(modifier)))


def test_delete_count_5():
    text = 'abc' * 100
    modifier = TextDelete(count=5).modify(text)
    assert_equal(6, len(list(modifier)))


def test_insert_nothing_new():
    text = string.printable * 100
    modifier = TextInsert().modify(text)
    for modified in modifier:
        for c in modified:
            assert_in(c, string.printable)


def test_insert_blocksize_default():
    text = 100 * 'a'
    modifier = TextInsert().modify(text)
    for inserted in modifier:
        pass
    assert_equal(len(text) + 20, len(inserted))


def test_insert_blocksize_2():
    text = 100 * 'a'
    modifier = TextInsert(blocksize=2).modify(text)
    for inserted in modifier:
        pass
    assert_equal(len(text) + 20*2, len(inserted))


def test_insert_count_default():
    text = 'abc' * 100
    modifier = TextInsert().modify(text)
    assert_equal(21, len(list(modifier)))


def test_insert_count_5():
    text = 'abc' * 100
    modifier = TextInsert(count=5).modify(text)
    assert_equal(6, len(list(modifier)))


def test_substitute_replaces():
    text = 'ab'
    modifier = TextSubstitute().modify(text)
    for substituted in modifier:
        assert_in(substituted, ['aa', 'ab', 'ba', 'bb'])


def test_substitute_blocksize_default():
    text = 100 * 'a'
    modifier = TextSubstitute().modify(text)
    for substituted in modifier:
        pass
    assert_equal(len(text), len(substituted))


def test_substitute_blocksize_2():
    text = 100 * 'a'
    modifier = TextSubstitute(blocksize=2).modify(text)
    for substituted in modifier:
        pass
    assert_equal(len(text), len(substituted))


def test_substitute_count_default():
    text = 'abc' * 100
    modifier = TextSubstitute().modify(text)
    assert_equal(21, len(list(modifier)))


def test_substitute_count_5():
    text = 'abc' * 100
    modifier = TextSubstitute(count=5).modify(text)
    assert_equal(6, len(list(modifier)))
