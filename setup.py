try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

desc = 'An exploratory project into how to compress Web ARChive (.warc) files.'

config = {
    'description': desc,
    'author': 'William Mayor',
    'url': 'webarchive.williammayor.co.uk',
    'download_url': 'https://github.com/WilliamMayor/warc-compression',
    'author_email': 'w.mayor@ucl.ac.uk',
    'version': '0.1',
    'install_requires': ['nose', 'tabulate'],
    'packages': ['warcompress'],
    'scripts': [],
    'name': 'warc-compression'
}

setup(**config)
