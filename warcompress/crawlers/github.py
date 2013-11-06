import sys
import os
import subprocess
import time
import datetime

import requests


DEVNULL = open(os.devnull, 'wb')
HEADERS = {'accept': 'application/vnd.github.preview'}


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


def crawl(dest_dir):
    done_path = os.path.join(dest_dir, 'done.txt')
    done = load_done(done_path)
    url = {
        'base': 'https://api.github.com/search/repositories',
        'q': '.github.io',
        '_in': 'name',
        'created': datetime.date(2008, 12, 17),
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
            time.sleep(60*5)
        except Exception as e:
            print 'There was an error:', e
            print 'GitHub response:'
            print response


def git_rev_list(path):
    try:
        result = subprocess.check_output(
            [
                'git',
                '--git-dir',
                '%s/.git' % (path, ),
                'rev-list',
                'master'
            ]
        )
        return result.splitlines()
    except:
        return []


def git_checkout(path, commit):
    rc = subprocess.call(
        [
            'git',
            '--git-dir',
            '%s/.git' % (path, ),
            'checkout',
            commit
        ],
        stdout=DEVNULL,
        stderr=subprocess.STDOUT
    )
    if rc != 0:
        raise Exception('git checkout failed')


def jekyll_serve(path):
    p = subprocess.Popen(
        [
            'jekyll',
            'serve',
            '--source',
            path,
            '--destination',
            '%s/_site' % (path, )
        ],
        stdout=subprocess.PIPE,
        stderr=DEVNULL
    )
    output = ''
    match = 'Server running'
    while output[-len(match):] != match and p.poll() is None:
        output += p.stdout.read(1)
    return p


def jekyll_kill(p):
    p.terminate()


def process(data_dir):
    print 'Processing crawled data'
    for user in os.walk(data_dir).next()[1]:
        print '  Processing user', user
        user_path = os.path.join(data_dir, user)
        for repo in os.walk(user_path).next()[1]:
            print '    Processing repo', repo
            repo_path = os.path.join(user_path, repo)
            for commit in git_rev_list(repo_path):
                try:
                    print '      Checking out commit', commit
                    git_checkout(repo_path, commit)
                    print '      Starting jekyll'
                    p = jekyll_serve(repo_path)
                    #heritrix_crawl()
                    print '      Killing jekyll'
                    jekyll_kill(p)
                    #jekyll_clean(repo_path)
                except Exception as e:
                    print '      Failed:', e


if __name__ == '__main__':
    data_dir = sys.argv[2]
    if sys.argv[1] == 'crawl':
        crawl(data_dir)
    elif sys.argv[1] == 'process':
        process(data_dir)
