import string

from nose.tools import assert_in, assert_equals

from warcompress.modifiers.css import (
    CssDelete,
    CssInsertDeclaration,
    CssInsertRule
)


def test_delete():
    text = 'h1{color:blue}h2{color:red}'
    modifier = CssDelete().modify(text)
    modified = modifier.next()
    modified = modifier.next()
    modified = modified.translate(None, string.whitespace)
    assert_in(modified, ['h1{color:blue}', 'h2{color:red}'])


def test_delete_all():
    text = 'h1{color:blue}h2{color:red}'
    modifier = CssDelete().modify(text)
    for modified in modifier:
        modified = modified.translate(None, string.whitespace)
    assert_equals('', modified)


def test_insert_declaration():
    text = 'h1{color:blue}h2{color:red}'
    modifier = CssInsertDeclaration().modify(text)
    modified = modifier.next()
    modified = modifier.next()
    modified = modified.translate(None, string.whitespace)
    assert_in(modified, ['h1{color:blue;color:red}h2{color:red}',
                         'h1{color:blue;color:blue}h2{color:red}',
                         'h1{color:blue}h2{color:red;color:blue}',
                         'h1{color:blue}h2{color:red;color:red}'])


def test_insert_rule():
    text = 'h1{color:blue}h2{color:red}'
    modifier = CssInsertRule().modify(text)
    modified = modifier.next()
    modified = modifier.next()
    modified = modified.translate(None, string.whitespace)
    assert_in(modified, ['h1{color:blue}h2{color:red}h1{color:blue}',
                         'h1{color:blue}h2{color:red}h1{color:red}',
                         'h1{color:blue}h2{color:red}h2{color:blue}',
                         'h1{color:blue}h2{color:red}h2{color:red}'])
