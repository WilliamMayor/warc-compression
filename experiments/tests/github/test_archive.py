import os
import shutil
import tempfile

import sh

from nose.tools import assert_equals, assert_not_equal, assert_true, assert_is_none, assert_in

import github.archive.archive as A

from tests.assets.index import index

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
                        fd.write(index.format(i=i))
                    sh.git('add', '-A')
                    sh.git('commit', '-m', 'c')


    def tearDown(self):
        shutil.rmtree(self.crawl_dir)

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
        assert_equals(2, len(A.rev_list(path)))