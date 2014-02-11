import re
import shutil
import socket
import gzip
import os
import time
import datetime
import sys

import hapy
import sh

from template import t

DEVNULL = open(os.devnull, 'wb')
HERITRIX_URL = 'https://localhost:8443'


def load_done(path):
    try:
        with open(path, 'r') as fd:
            return set(fd.read().splitlines())
    except:
        return set()


def save_done(path, done):
    with open(path, 'w') as fd:
        fd.write('\n'.join(done))
        fd.flush()


def git_rev_list(path):
    try:
        return sh.git('rev-list', 'master', _cwd=path).strip().splitlines()
    except Exception:
        return []


def git_checkout(path, commit):
    sh.git.checkout(commit, _cwd=path)


def jekyll_serve(path, port):
    jekyll = sh.jekyll.serve(
        port=port, _cwd=path, _iter=True, _bg=True)
    for line in jekyll:
        if 'Server running' in line:
            return jekyll
    raise Exception('Jekyll did not start serving')


def wait_for(h, job_name, func_name):
    info = h.get_job_info(job_name)
    while func_name not in info['job']['availableActions']['value']:
        time.sleep(1)
        info = h.get_job_info(job_name)


def commit_date(path, commit):
    d = sh.git.show(commit, s=True, format='%ci', _cwd=path, _tty_out=False)
    return datetime.datetime.strptime(
        repr(d).strip()[0:19], '%Y-%m-%d %H:%M:%S')


def rewrite_warc(name, date):
    h = hapy.Hapy(
        HERITRIX_URL,
        username='admin',
        password=sys.argv[2],
        timeout=10.0)
    info = h.get_job_info(name)
    job_path = os.path.dirname(info['job']['primaryConfig'])
    warcs_dir = os.path.join(job_path, 'latest', 'warcs')
    for warc in os.walk(warcs_dir).next()[2]:
        path = os.path.join(warcs_dir, warc)
        gzr = gzip.open(path, 'r')
        content = gzr.read()
        gzr.close()
        content = re.sub(
            '^WARC-Date: .*?\n', 'WARC-Date: %s\n' % date.isoformat(),
            content,
            flags=re.MULTILINE
        )
        gzw = gzip.open(path, 'w')
        gzw.write(content)
        gzw.close()


def heritrix_crawl(name):
    h = hapy.Hapy(
        HERITRIX_URL,
        username='admin',
        password=sys.argv[2],
        timeout=10.0)
    try:
        print '        Building'
        h.build_job(name)
        wait_for(h, name, 'launch')
        print '        Launching'
        h.launch_job(name)
        wait_for(h, name, 'unpause')
        print '        Unpausing'
        h.unpause_job(name)
        print '        Waiting for finish'
        sys.stdout.write('        ')
        info = h.get_job_info(name)
        finished = None
        while finished is None:
            sys.stdout.write('.')
            sys.stdout.flush()
            info = h.get_job_info(name)
            try:
                finished = info['job']['crawlExitStatus']
            except:
                time.sleep(30)
        sys.stdout.write('\n')
        sys.stdout.flush()
        print '        Tearing down'
        h.teardown_job(name)
        wait_for(h, name, 'build')
    except:
        try:
            h.terminate_job(name)
            wait_for(h, name, 'teardown')
            h.teardown_job(name)
        except:
            pass


def process_commit(repo_path, name, commit, port, attempts=5):
    try:
        print '      Checking out commit', commit
        git_checkout(repo_path, commit)
        print '      Starting jekyll'
        p = jekyll_serve(repo_path, port)
        print '      Running heritrix job'
        heritrix_crawl(name)
        rewrite_warc(name, commit_date(repo_path, commit))
        return
    finally:
        try:
            print '      Killing jekyll'
            p.terminate()
        except:
            pass
        try:
            print '      Cleaning jekyll directory'
            shutil.rmtree('%s/_site' % (repo_path, ))
        except:
            pass
    if attempts > 0:
        print '      Failed, trying again'
        time.sleep(60)
        process_commit(repo_path, name, commit, port, attempts - 1)


def process_repo(repo_path, name):
    print '    Creating heritrix job'
    h = hapy.Hapy(
        HERITRIX_URL,
        username='admin',
        password=sys.argv[2],
        timeout=10.0)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()
    cxml = t.replace('WC_PORT', str(port))
    h.create_job(name)
    h.submit_configuration(name, cxml)
    wait_for(h, name, 'build')
    for commit in git_rev_list(repo_path):
        process_commit(repo_path, name, commit, port)
    git_checkout(repo_path, 'master')


def process(data_dir):
    print 'Processing crawled data'
    done_path = os.path.join(data_dir, 'archived.txt')
    done = load_done(done_path)
    for user in os.walk(data_dir).next()[1]:
        user_path = os.path.join(data_dir, user)
        for repo in os.walk(user_path).next()[1]:
            try:
                name = '%s-%s' % (user, repo)
                print '  Processing', name
                if name in done:
                    print '    Already archived'
                    continue
                repo_path = os.path.join(user_path, repo)
                process_repo(repo_path, name)
                done.add(name)
                save_done(done_path, done)
                return
            except Exception as e:
                print '    Error', str(e)

if __name__ == '__main__':
    print sys.argv[2]
    process(sys.argv[1])
