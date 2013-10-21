import string

from nose.tools import assert_in, assert_equals

from warcompress.modifiers.css import (
    remove_rule,
    remove_declaration
)


def test_remove_rule():
    text = 'h1{color:blue}h2{color:red}'
    modified = remove_rule.modify(text, 0.5).translate(None, string.whitespace)
    assert_in(modified, ['h1{color:blue}', 'h2{color:red}'])


def test_remove_rule_all():
    text = 'h1{color:blue}h2{color:red}'
    modified = remove_rule.modify(text, 1).translate(None, string.whitespace)
    assert_equals(modified, '')


def test_remove_declaration():
    text = 'h1{color:blue;font-size:2em}'
    modified = remove_declaration.modify(text, 0.5)
    modified = modified.translate(None, string.whitespace)
    assert_in(modified, ['h1{color:blue}', 'h1{font-size:2em}'])


def test_remove_declaration_empty():
    text = 'h1{color:blue;font-size:2em}'
    modified = remove_declaration.modify(text, 1)
    modified = modified.translate(None, string.whitespace)
    assert_equals(modified, '')
