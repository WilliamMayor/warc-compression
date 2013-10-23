import sys
import os
import subprocess
import time

import requests


DEVNULL = open(os.devnull, 'wb')


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


def read_set(path):
    try:
        with open(path, 'r') as fd:
            return set(fd.read().splitlines())
    except:
        return set()


if __name__ == '__main__':
    dest_dir = sys.argv[1]

    done_path = os.path.join(dest_dir, 'done.txt')
    done = read_set(done_path)

    headers = {'accept': 'application/vnd.github.preview'}
    url = 'https://api.github.com/search/repositories?q=github.io&in%3Aname'

    while url is not None:
        print 'Running search'
        r = requests.get(url, headers=headers)
        url = None
        try:
            results = r.json()
            print '    Found %d repos' % len(results['items'])
            sys.stdout.write('    >')
            sys.stdout.flush()
            for repo in results['items']:
                git_url = repo['git_url']
                if git_url not in done:
                    path = os.path.join(dest_dir, repo['full_name'])
                    success = clone(git_url, path)
                    if success:
                        done.add(git_url)
                        sys.stdout.write('.')
                    else:
                        sys.stdout.write('x')
                    time.sleep(30)
                else:
                    sys.stdout.write(' ')
                sys.stdout.flush()
            sys.stdout.write('<\n')
            sys.stdout.flush()

            with open(done_path, 'w') as fd:
                fd.write('\n'.join(done))

            rate_limit = int(r.headers['x-ratelimit-remaining'])
            now = int(time.time())
            reset = int(r.headers['x-ratelimit-reset'])
            wait = reset - now + 1
            if rate_limit == 0 and wait > 0:
                print '    Hit rate limit, waiting for %d secs' % wait
                time.sleep(wait)

            url = r.links['next']['url']
        except:
            pass
