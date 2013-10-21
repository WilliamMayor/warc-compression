import string

from nose.tools import assert_equal, assert_in, assert_not_equal
from warcompress.modifiers.text import (
    delete,
    insert,
    substitute
)


def test_delete_defaults():
    text = 100 * 'a'
    deleted = delete.modify(text)
    assert_equal(len(text) - 1, len(deleted))


def test_delete_half():
    text = 'abcd'
    deleted = delete.modify(text, 0.5)
    assert_in(deleted, ['ab', 'ac', 'ad', 'bc', 'bd', 'cd'])


def test_delete_all():
    text = 'abc'
    deleted = delete.modify(text, 1)
    assert_equal(0, len(deleted))


def test_delete_more():
    text = 'abc'
    deleted = delete.modify(text, 2)
    assert_equal(0, len(deleted))


def test_insert_nothing_new():
    text = string.printable * 100
    inserted = insert.modify(text)
    for c in inserted:
        assert_in(c, string.printable)


def test_insert_defaults():
    text = 100 * 'a'
    inserted = insert.modify(text)
    assert_equal(len(text) + 1, len(inserted))


def test_insert_double():
    text = 'abc'
    inserted = insert.modify(text, 1)
    assert_equal(2 * len(text), len(inserted))


def test_insert_triple():
    text = 'abc'
    inserted = insert.modify(text, 2)
    assert_equal(3 * len(text), len(inserted))


def test_substitute():
    text = 'abcde' * 40
    sub = substitute.modify(text)
    assert_equal(len(text), len(sub))
    assert_not_equal(text, sub,
                     ('This might fail due to the'
                      ' randomness of the substitution.'
                      ' i.e. exactly the same thing was'
                      ' taken out and then put back in'))
