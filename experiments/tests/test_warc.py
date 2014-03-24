import sqlite3
import time
import os
import shutil
import tempfile

from nose.tools import assert_equals, assert_not_equal, assert_true, assert_is_none, assert_not_in

from github.WARC import WARC
import tests.assets.index as index

class TestWARC:

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        d = os.path.dirname(os.path.abspath(index.__file__))
        p = os.path.join(d, 'example.warc.gz')
        q = os.path.join(self.tempdir, 'example.warc.gz')
        shutil.copy(p, q)
        self.w = WARC(q)


    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_iter(self):
        assert_equals(10, len(list(self.w)))

    def test_no_repeats(self):
        d = os.path.dirname(os.path.abspath(index.__file__))
        p = os.path.join(d, 'example.warc.gz')
        for r in WARC(p):
            self.w.add(r)
        s = set()
        for r in self.w:
            assert_not_in(r.headers['WARC-Record-ID'], s)
            s.add(r.headers['WARC-Record-ID'])
