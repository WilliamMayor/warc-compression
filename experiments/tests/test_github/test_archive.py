import re
import os
import shutil
import tempfile

import sh

from nose.tools import assert_equals, assert_not_equal, assert_true, assert_is_none, assert_in

import github.archive.archive as A
import tests.assets.index as index

from github.WARC import WARC

class TestRun:

    def setUp(self):
        self.crawl_dir = tempfile.mkdtemp()
        self.job_paths = []
        self.job_names = []
        for _ in xrange(5):
            u = tempfile.mkdtemp(dir=self.crawl_dir)
            for _ in xrange(2):
                p = tempfile.mkdtemp(dir=u)
                self.job_paths.append(p)
                self.job_names.append('%s-%s' % (u[len(self.crawl_dir)+1:], p[len(u)+1:]))
                sh.git('init', _cwd=p)
                for i in xrange(2):
                    with open(os.path.join(p, 'index.html'), 'w') as fd:
                        fd.write(index.index.format(i=i))
                    sh.git('add', '-A', _cwd=p)
                    sh.git('commit', '-m', 'c', _cwd=p)
        self.warcs_dir = tempfile.mkdtemp()


    def tearDown(self):
        shutil.rmtree(self.crawl_dir)
        shutil.rmtree(self.warcs_dir)

    def test_find_job(self):
        name, path = A.find_job(self.crawl_dir, [], [])
        assert_in(path, self.job_paths)
        assert_in(name, self.job_names)

    def test_find_job_not_current(self):
        name, path = A.find_job(self.crawl_dir, [], self.job_names)
        assert_is_none(name)
        assert_is_none(path)
        name, path = A.find_job(self.crawl_dir, [], self.job_names[:-1])
        assert_equals(path, self.job_paths[-1])

    def test_find_job_not_done(self):
        name, path = A.find_job(self.crawl_dir, self.job_names, [])
        assert_is_none(name)
        assert_is_none(path)
        name, path = A.find_job(self.crawl_dir, self.job_names[:-1], [])
        assert_equals(path, self.job_paths[-1])

    def test_rev_list(self):
        name, path = A.find_job(self.crawl_dir, [], [])
        assert_equals(2, len(A.git_rev_list(path)))

    def test_filter_records(self):
        d = os.path.dirname(os.path.abspath(index.__file__))
        records = list(A.filter_records(d))
        assert_equals(4, len(records))
        warcinfo = [r for r in records if r.headers['WARC-Type'] == 'warcinfo']
        assert_equals(1, len(warcinfo))
        requests = [r for r in records if r.headers['WARC-Type'] == 'request']
        assert_equals(1, len(requests))
        assert_true(re.match('^http://localhost:\d+/$', requests[0].headers['WARC-Target-URI']))
        responses = [r for r in records if r.headers['WARC-Type'] == 'response']
        assert_equals(1, len(responses))
        assert_true(re.match('^http://localhost:\d+/$', responses[0].headers['WARC-Target-URI']))

    def test_main(self):
        A.main(self.crawl_dir, self.warcs_dir, '8700mm0n9wnxkjc1cvtbdhdqyxlji7z0')
        files = os.walk(self.warcs_dir).next()[2]
        assert_equals(1, len(files))
        path = os.path.join(self.warcs_dir, files[0])
        records = list(WARC(path))
        assert_equals(7, len(records))

    def test_main_two_repos(self):
        A.main(self.crawl_dir, self.warcs_dir, '8700mm0n9wnxkjc1cvtbdhdqyxlji7z0')
        A.main(self.crawl_dir, self.warcs_dir, '8700mm0n9wnxkjc1cvtbdhdqyxlji7z0')
        files = os.walk(self.warcs_dir).next()[2]
        assert_equals(1, len(files))
        path = os.path.join(self.warcs_dir, files[0])
        records = list(WARC(path))
        assert_equals(13, len(records))
