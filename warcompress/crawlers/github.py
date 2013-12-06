import re
import sys
import os
import subprocess
import time
import datetime
import shutil
import socket
import gzip

from pkg_resources import resource_string

import requests
import hapy


DEVNULL = open(os.devnull, 'wb')
HEADERS = {'accept': 'application/vnd.github.preview'}
HERITRIX_URL = 'https://localhost:8443'


def clone(git_url, path):
    try:
        rc = subprocess.call(
            [
                'git',
                'clone',
                git_url,
                path
            ],
            stdout=DEVNULL,
            stderr=subprocess.STDOUT
        )
        return rc == 0
    except:
        return False


def load_done(path):
    try:
        with open(path, 'r') as fd:
            return set(fd.read().splitlines())
    except:
        return set()


def save_done(path, done):
    with open(path, 'w') as fd:
        fd.write('\n'.join(done))


def rate_limit(response):
    try:
        headers = response.headers
        rate_limit = int(headers['x-ratelimit-remaining'])
        now = int(time.time())
        reset = int(headers['x-ratelimit-reset'])
        wait = reset - now + 1
        if rate_limit == 0 and wait > 0:
            print 'Hit Search API rate limit, waiting for %d secs' % wait
            time.sleep(wait)
    except:
        time.sleep(30)


def next_url(url, response):
    try:
        response.links['next']['url']
        url['page'] = url['page'] + 1
    except:
        url['page'] = 1
        if url['created'] == datetime.date.today():
            url = None
        else:
            url['created'] = url['created'] + datetime.timedelta(days=1)
    return url


def build_url(parts):
    url = parts['base'] + '?'
    url += '+'.join([
        'q=%s' % (parts['q'], ),
        'in%%3A%s' % (parts['_in'], ),
        'created%%3A%s..%s' % (
            parts['created'],
            parts['created'] + datetime.timedelta(days=1)
        ),
    ])
    url += '&page=%d' % (parts['page'], )
    return url


def crawl(dest_dir, from_):
    done_path = os.path.join(dest_dir, 'done.txt')
    done = load_done(done_path)
    url = {
        'base': 'https://api.github.com/search/repositories',
        'q': '.github.io',
        '_in': 'name',
        'created': datetime.date(from_[0], from_[1], from_[2]),
        'page': 1
    }
    while url is not None:
        u = build_url(url)
        print 'Searching', u
        response = None
        try:
            response = requests.get(u, headers=HEADERS)
            repos = response.json()['items']
            print '  Found', len(repos)
            sys.stdout.write('  ')
            for repo in repos:
                git_url = repo['git_url']
                if git_url not in done:
                    path = os.path.join(dest_dir, repo['full_name'])
                    success = clone(git_url, path)
                    if success:
                        done.add(git_url)
                        sys.stdout.write('c')
                    else:
                        sys.stdout.write('e')
                    time.sleep(30)
                else:
                    sys.stdout.write('d')
                sys.stdout.flush()
            sys.stdout.write('\n')
            save_done(done_path, done)
            url = next_url(url, response)
            rate_limit(response)
        except requests.exceptions.ConnectionError:
            time.sleep(60 * 5)
        except Exception as e:
            print 'There was an error:', e
            print 'GitHub response:'
            print response


def git_rev_list(path):
    try:
        args = [
            'git',
            'rev-list',
            'master'
        ]
        result = subprocess.check_output(args, cwd=path)
        return result.splitlines()
    except Exception as e:
        raise e
        return []


def git_checkout(path, commit):
    rc = subprocess.call(
        [
            'git',
            'checkout',
            commit
        ],
        stdout=DEVNULL,
        stderr=subprocess.STDOUT,
        cwd=path
    )
    if rc != 0:
        raise Exception('git checkout failed')


def jekyll_serve(path, port):
    p = subprocess.Popen(
        [
            'jekyll',
            'serve',
            '--port',
            str(port)
        ],
        stdout=subprocess.PIPE,
        stderr=DEVNULL,
        cwd=path
    )
    output = ''
    match = 'Server running'
    while output[-len(match):] != match and p.poll() is None:
        output += p.stdout.read(1)
    if p.poll() is not None:
        raise Exception('could not start jekyll')
    return p


def wait_for(h, job_name, func_name):
    info = h.get_job_info(job_name)
    while func_name not in info['job']['availableActions']['value']:
        time.sleep(1)
        info = h.get_job_info(job_name)


def commit_date(path, commit):
    args = [
        'git',
        'show',
        '-s',
        '--format="%ci"',
        commit
    ]
    return datetime.datetime.strptime(
        subprocess.check_output(args, cwd=path)[1:20],
        '%Y-%m-%d %H:%M:%S'
    )


def rewrite_warc(name, port, date):
    h = hapy.Hapy(HERITRIX_URL, username='admin', password=sys.argv[3])
    info = h.get_job_info(name)
    job_path = os.path.dirname(info['job']['primaryConfig'])
    warcs_dir = os.path.join(job_path, 'latest', 'warcs')
    for warc in os.walk(warcs_dir).next()[2]:
        path = os.path.join(warcs_dir, warc)
        gzr = gzip.open(path, 'r')
        content = gzr.read()
        gzr.close()
        content = content.replace(
            'http://localhost:%d' % port,
            'http://%s' % name
        )
        content = re.sub(
            '^WARC-Date: .*?\n', 'WARC-Date: %s\n' % date.isoformat(),
            content,
            flags=re.MULTILINE
        )
        gzw = gzip.open(path, 'w')
        gzw.write(content)
        gzw.close()


def heritrix_crawl(name):
    h = hapy.Hapy(HERITRIX_URL, username='admin', password=sys.argv[3])
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
        rewrite_warc(name, port, commit_date(repo_path, commit))
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
    h = hapy.Hapy(HERITRIX_URL, username='admin', password=sys.argv[3])
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()
    cxml = resource_string(
        'warcompress',
        'assets/template.cxml').replace('WC_PORT', str(port))
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
                repo_path = os.path.join(user_path, repo)
                process_repo(repo_path, name)
                done.add(name)
                save_done(done_path, done)
                return
            except Exception as e:
                print '    Error', str(e)


def safe_get(l, i, d):
    try:
        return l[i]
    except:
        return d


if __name__ == '__main__':
    data_dir = sys.argv[2]
    if sys.argv[1] == 'crawl':
        from_ = (
            safe_get(sys.argv, 2, 2008),
            safe_get(sys.argv, 3, 12),
            safe_get(sys.argv, 4, 17)
        )
        crawl(data_dir, from_)
    elif sys.argv[1] == 'process':
        process(data_dir)
