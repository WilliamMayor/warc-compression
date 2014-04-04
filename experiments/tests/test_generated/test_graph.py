import sqlite3
import time
import os
import shutil
import tempfile

from nose.tools import assert_equals, assert_not_equal, assert_true, assert_is_none

import generated.graph as G

class TestGraph:

    def test_data_combine_similar(self):
        data = [
            ('d', 'c', 0.0, 1, 1, 1, 1),
            ('d', 'c', 0.0, 2, 2, 2, 2)
        ]
        assert_equals([('d', 'c', 0.0, ([1, 2], [1, 2], [1, 2]))], G.data(data))

    def test_data_combine_multiple(self):
        data = [
            ('d', 'c', 0.0, 1, 1, 1, 1),
            ('d', 'c', 0.0, 2, 2, 2, 2),
            ('d1', 'c', 0.0, 1, 1, 1, 1),
            ('d1', 'c', 0.0, 2, 2, 2, 2)
        ]
        assert_equals([('d', 'c', 0.0, ([1, 2], [1, 2], [1, 2])), ('d1', 'c', 0.0, ([1, 2], [1, 2], [1, 2]))], G.data(data))

    def test_data_filter_mutate(self):
        data = [
            ('d', 'c', 0.0, 1, 1, 1, 1),
            ('d', 'c', 0.1, 2, 2, 2, 2)
        ]
        assert_equals([('d', 'c', 0.0, ([1], [1], [1]))], G.data(data, mutate=0.0))

    def test_data_filter_delta(self):
        data = [
            ('d', 'c', 0.0, 1, 1, 1, 1),
            ('d1', 'c', 0.0, 2, 2, 2, 2)
        ]
        assert_equals([('d', 'c', 0.0, ([1], [1], [1]))], G.data(data, delta='d'))

    def test_data_filter_compression(self):
        data = [
            ('d', 'c', 0.0, 1, 1, 1, 1),
            ('d', 'c1', 0.1, 2, 2, 2, 2)
        ]
        assert_equals([('d', 'c', 0.0, ([1], [1], [1]))], G.data(data, compression='c'))