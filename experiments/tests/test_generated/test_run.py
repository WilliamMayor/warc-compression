import sqlite3
import time
import os
import shutil
import tempfile

from nose.tools import assert_equals, assert_not_equal, assert_true, assert_is_none

import generated.run as R

class TestRun:

    def setUp(self):
        db = ':memory:'
        data = tempfile.mkdtemp()
        self.E = R.Experiment(db, data, 0.1)
        self.E.data_size = 1
        self.E.trial_count = 2
        self.E.step_count = 2

    def tearDown(self):
        shutil.rmtree(self.E.data_dir)
        try:
            os.remove(self.E.db_path)
        except:
            # only in memory
            pass

    def count_files(self, path=None):
        if path is None:
            path = self.E.data_dir
        count = 0
        for _, _, files in os.walk(path):
            count += len(files)
        return count

    def compress_wait(self, t):
        def wait(content):
            time.sleep(t)
            return content
        return wait

    def difference_wait(self, t):
        def wait(a, b):
            time.sleep(t)
            return None
        return wait

    def test_count_files(self):
        assert_equals(0, self.count_files())
        tempfile.mkstemp(dir=self.E.data_dir)
        assert_equals(1, self.count_files())
        d = tempfile.mkdtemp(dir=self.E.data_dir)
        tempfile.mkstemp(dir=d)
        assert_equals(2, self.count_files())

    def test_compress_wait(self):
        tick = time.time()
        a = 'a'
        assert_equals(a, self.compress_wait(1)(a))
        assert_equals(1, int(time.time() - tick))

    def test_compressor(self):
        c = 'content'
        do = self.compress_wait(1)
        undo = self.compress_wait(2)
        n, l, t1, t2 = self.E.compressor(do, undo, c)
        assert_equals('wait', n)
        assert_equals(len(c), l)
        assert_equals(1, int(t1))
        assert_equals(2, int(t2))

    def test_differencer(self):
        do = self.difference_wait(1)
        undo = self.difference_wait(2)
        n, d, t1, t2 = self.E.differencer(do, undo, None, None)
        assert_equals('wait', n)
        assert_is_none(d)
        assert_equals(1, int(t1))
        assert_equals(2, int(t2))

    def test_zpaq_leak(self):
        c = self.count_files()
        self.E.zpaq('test')
        assert_equals(c, self.count_files())

    def test_un_zpaq_leak(self):
        c = self.count_files()
        self.E.un_zpaq(self.E.zpaq('test'))
        assert_equals(c, self.count_files())

    def test_zpaq_unzpaq(self):
        c = 'test'
        assert_equals(c, self.E.un_zpaq(self.E.zpaq(c)))

    def test_bzip2_leak(self):
        c = self.count_files()
        self.E.bzip2('test')
        assert_equals(c, self.count_files())

    def test_un_bzip2_leak(self):
        c = self.count_files()
        self.E.un_bzip2(self.E.bzip2('test'))
        assert_equals(c, self.count_files())

    def test_bzip2_unbzip2(self):
        c = 'test'
        assert_equals(c, self.E.un_bzip2(self.E.bzip2(c)))

    def test_gzip_leak(self):
        c = self.count_files()
        self.E.gzip('test')
        assert_equals(c, self.count_files())

    def test_un_gzip_leak(self):
        c = self.count_files()
        self.E.un_gzip(self.E.gzip('test'))
        assert_equals(c, self.count_files())

    def test_no_compression_leak(self):
        c = self.count_files()
        self.E.no_compression('test')
        assert_equals(c, self.count_files())

    def test_no_compression_un_no_compression(self):
        c = 'test'
        assert_equals(c, self.E.no_compression(self.E.no_compression(c)))

    def test_gzip_ungzip(self):
        c = 'test'
        assert_equals(c, self.E.un_gzip(self.E.gzip(c)))

    def test_no_delta_leak(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        c = self.count_files()
        self.E.no_delta(fd1, fd2)
        assert_equals(c, self.count_files())

    def test_unno_delta_leak(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        c = self.count_files()
        self.E.un_no_delta(fd1, self.E.no_delta(fd1, fd2))
        assert_equals(c, self.count_files())

    def test_no_delta_un_no_delta(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        assert_equals('second', self.E.un_no_delta(fd1, self.E.no_delta(fd1, fd2)))

    def test_no_delta_side_effects(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        self.E.no_delta(fd1, fd2)
        fd1.seek(0)
        fd2.seek(0)
        assert_equals('first', fd1.read())
        assert_equals('second', fd2.read())

    def test_unno_delta_side_effects(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        self.E.un_no_delta(fd1, self.E.no_delta(fd1, fd2))
        fd1.seek(0)
        fd2.seek(0)
        assert_equals('first', fd1.read())
        assert_equals('second', fd2.read())

    def test_diff_leak(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        c = self.count_files()
        self.E.diff(fd1, fd2)
        assert_equals(c, self.count_files())

    def test_undiff_leak(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        c = self.count_files()
        self.E.un_diff(fd1, self.E.diff(fd1, fd2))
        assert_equals(c, self.count_files())

    def test_diff_un_diff(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        assert_equals('second', self.E.un_diff(fd1, self.E.diff(fd1, fd2)))

    def test_diff_side_effects(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        self.E.diff(fd1, fd2)
        fd1.seek(0)
        fd2.seek(0)
        assert_equals('first', fd1.read())
        assert_equals('second', fd2.read())

    def test_undiff_side_effects(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        self.E.un_diff(fd1, self.E.diff(fd1, fd2))
        fd1.seek(0)
        fd2.seek(0)
        assert_equals('first', fd1.read())
        assert_equals('second', fd2.read())

    def test_diff_newline(self):
        fd1 = self.E.write('first\n')
        fd2 = self.E.write('second\n')
        # If there isn't a new line at the end of the file diff complains
        # If there is a new line then it eats it when applying the patch
        # I don't think this will affect many files, those that it does will
        # only be changes by a single byte.
        assert_equals('second', self.E.un_diff(fd1, self.E.diff(fd1, fd2)))

    def test_vcdiff_leak(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        c = self.count_files()
        self.E.vcdiff(fd1, fd2)
        assert_equals(c, self.count_files())

    def test_unvcdiff_leak(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        c = self.count_files()
        self.E.un_vcdiff(fd1, self.E.vcdiff(fd1, fd2))
        assert_equals(c, self.count_files())

    def test_vcdiff_un_vcdiff(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        assert_equals('second', self.E.un_vcdiff(fd1, self.E.vcdiff(fd1, fd2)))

    def test_vcdiff_side_effects(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        self.E.vcdiff(fd1, fd2)
        fd1.seek(0)
        fd2.seek(0)
        assert_equals('first', fd1.read())
        assert_equals('second', fd2.read())

    def test_unvcdiff_side_effects(self):
        fd1 = self.E.write('first')
        fd2 = self.E.write('second')
        self.E.un_vcdiff(fd1, self.E.vcdiff(fd1, fd2))
        fd1.seek(0)
        fd2.seek(0)
        assert_equals('first', fd1.read())
        assert_equals('second', fd2.read())

    def test_run(self):
        _, db_path = tempfile.mkstemp()
        self.E.db_path = db_path
        self.E.run()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(DISTINCT mutate), COUNT(DISTINCT trial), COUNT(DISTINCT step) from delta')
        m, t, s = cursor.fetchone()
        assert_equals(1, m)
        assert_equals(2, t)
        assert_equals(2, s)


    def test_new_path(self):
        for _ in xrange(10):
            assert_not_equal(self.E.new_path(), self.E.new_path())

    def test_new_path_suffix(self):
        assert_true(self.E.new_path(suffix='test').endswith('test'))

    def test_trial_leaves_no_files(self):
        c = self.count_files()
        self.E.trial()
        assert_equals(c, self.count_files())

    def test_write(self):
        c = 'test content'
        fd = self.E.write(c)
        fd.seek(0)
        assert_equals(c, fd.read())

    def test_write_suffix(self):
        c = 'test content'
        s = 'suffix'
        fd = self.E.write(c, suffix=s)
        fd.seek(0)
        assert_equals(c, fd.read())
        assert_true(fd.name.endswith(s))

    def test_write_append(self):
        c = 'test content'
        d = 'more'
        fd = self.E.write(c)
        self.E.write(d, fd=fd)
        fd.seek(0)
        assert_equals(c + d, fd.read())
