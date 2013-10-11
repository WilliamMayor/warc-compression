from nose.tools import assert_equal, assert_in
from warcompress.experiments.modifiers import identity, delete, insert


def setup():
    print "SETUP!"


def teardown():
    print "TEAR DOWN!"


def test_identity():
    text = 'abc'
    assert_equal(text, identity(text))


def test_delete_defaults():
    text = 'abc'
    deleted = delete(text)
    assert_equal(len(text) - 1, len(deleted))
    assert_in(deleted, ['bc', 'ac', 'ab'])


def test_delete_more():
    text = 'abc'
    deleted = delete(text, 2)
    assert_equal(len(text) - 2, len(deleted))
    assert_in(deleted, ['c', 'a'])


def test_delete_all():
    text = 'abc'
    deleted = delete(text, len(text))
    assert_equal(0, len(deleted))


def test_delete_too_much():
    text = 'abc'
    deleted = delete(text, len(text) + 1)
    assert_equal(0, len(deleted))


def test_insert_defaults():
    text = 'abc'
    inserted = insert(text)
    assert_equal(len(text) + 1, len(inserted))
    for c in inserted:
        assert_in(c, text)


def test_insert_more():
    text = 'abc'
    inserted = insert(text, 2)
    assert_equal(len(text) + 2, len(inserted))
    for c in inserted:
        assert_in(c, text)
