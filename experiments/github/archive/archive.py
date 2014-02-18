import re
import shutil
import socket
import gzip
import os
import time
import datetime
import sys
import traceback
import logging

import hapy
import sh

from template import t
from filelock import FileLock

DEVNULL = open(os.devnull, 'wb')
HERITRIX_URL = 'https://localhost:8443'
HERITRIX = None


def load_list(path, need_lock=True):
    """This is really ugly"""
    try:
        if need_lock:
            with FileLock(path):
                with open(path, 'r') as fd:
                    return set(fd.read().splitlines())
        else:
            with open(path, 'r') as fd:
                return set(fd.read().splitlines())
    except:
        return set()


def save_list(path, done, need_lock=True):
    if need_lock:
        with FileLock(path):
            with open(path, 'w') as fd:
                fd.write('\n'.join(done))
                fd.flush()
    else:
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


def wait_for(job_name, func_name):
    info = HERITRIX.get_job_info(job_name)
    while func_name not in info['job']['availableActions']['value']:
        time.sleep(1)
        info = HERITRIX.get_job_info(job_name)


def commit_date(path, commit):
    d = sh.git.show(commit, s=True, format='%ci', _cwd=path, _tty_out=False)
    return datetime.datetime.strptime(
        repr(d).strip()[0:19], '%Y-%m-%d %H:%M:%S')


def rewrite_warc(name, date):
    logger = logging.getLogger('archive-%s' % name)
    logger.info('re-writing warc files')
    info = HERITRIX.get_job_info(name)
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
    logger = logging.getLogger('archive-%s' % name)
    try:
        logger.info('Building')
        HERITRIX.build_job(name)
        wait_for(name, 'launch')
        logger.info('Launching')
        HERITRIX.launch_job(name)
        wait_for(name, 'unpause')
        logger.info('Unpausing')
        HERITRIX.unpause_job(name)
        logger.info('Waiting for finish')
        info = HERITRIX.get_job_info(name)
        finished = None
        while finished is None:
            logger.info('Not finished')
            info = HERITRIX.get_job_info(name)
            try:
                finished = info['job']['crawlExitStatus']
            except:
                time.sleep(30)
        logger.info('Tearing down')
        HERITRIX.teardown_job(name)
        wait_for(name, 'build')
    except:
        try:
            HERITRIX.terminate_job(name)
            wait_for(name, 'teardown')
            HERITRIX.teardown_job(name)
        except:
            pass


def process_commit(repo_path, name, commit, port):
    logger = logging.getLogger('archive-%s' % name)
    try:
        logger.info('Checking out commit: %s' % commit)
        git_checkout(repo_path, commit)
        logger.info('Starting jekyll')
        p = jekyll_serve(repo_path, port)
        logger.info('Running heritrix job')
        heritrix_crawl(name)
        rewrite_warc(name, commit_date(repo_path, commit))
        return
    except:
        pass
    finally:
        try:
            logger.info('Killing jekyll')
            p.terminate()
        except:
            pass
        try:
            logger.info('Cleaning jekyll directory')
            shutil.rmtree('%s/_site' % (repo_path, ))
        except:
            pass


def process_repo(repo_path, name):
    logger = logging.getLogger('archive-%s' % name)
    rev_list = git_rev_list(repo_path)
    if len(rev_list) > 0:
        logger.info('Creating heritrix job')
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', 0))
        port = s.getsockname()[1]
        s.close()
        cxml = t.replace('WC_PORT', str(port))
        HERITRIX.create_job(name)
        HERITRIX.submit_configuration(name, cxml)
        wait_for(name, 'build')
        for commit in rev_list:
            process_commit(repo_path, name, commit, port)
        git_checkout(repo_path, 'master')
        return True
    else:
        logger.info('No commits')
        return False


def find_job(data_dir, done, current):
    for user in os.walk(data_dir).next()[1]:
        user_path = os.path.join(data_dir, user)
        for repo in os.walk(user_path).next()[1]:
            name = '%s-%s' % (user, repo)
            if name in done or name in current:
                continue
            path = os.path.join(user_path, repo)
            return name, path
    return None, None


def process(data_dir):
    done_path = os.path.join(data_dir, 'archived.txt')
    done = load_list(done_path)
    current_path = os.path.join(data_dir, 'current.txt')
    with FileLock(current_path):
        current = load_list(current_path, need_lock=False)
        job, repo_path = find_job(data_dir, done, current)
        current.add(job)
        save_list(current_path, current, need_lock=False)
    if None not in [job, repo_path]:
        logger = logging.getLogger('archive-%s' % job)
        hdlr = logging.FileHandler('archive-%s.log' % job)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.INFO)
        try:
            process_repo(repo_path, job)
            done.add(job)
            save_list(done_path, done)
        except:
            logger.error('Error occurred')
            logger.error(traceback.format_exc())

if __name__ == '__main__':
    HERITRIX = hapy.Hapy(
        HERITRIX_URL,
        username='admin',
        password=sys.argv[2],
        timeout=10.0)
    process(sys.argv[1])
