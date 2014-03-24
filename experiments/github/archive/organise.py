import pprint
import sys

import hapy

import archive

h = hapy.Hapy(
        archive.HERITRIX_URL,
        username='admin',
        password=sys.argv[1],
        timeout=10.0)

pprint.pprint(h.get_info())