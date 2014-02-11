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
        fd.flush()


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


def safe_get(l, i, d):
    try:
        return l[i]
    except:
        return d

if __name__ == '__main__':
    data_dir = sys.argv[1]
    from_ = (
        safe_get(sys.argv, 2, 2008),
        safe_get(sys.argv, 3, 12),
        safe_get(sys.argv, 4, 17)
    )
    crawl(data_dir, from_)
