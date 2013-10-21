"""
How much compression can you get on a single file repeated 20 times
"""
import os
import sys
from warcompress.experiments.Experiment import Experiment
from warcompress.encodings import utilities


def modifier_factory(base_path, data_path):
    def g():
        with open(base_path, 'rb') as fd:
            base = fd.read()
            modified = base
            for i in xrange(1, 20):
                modified += utilities.RECORD_SEPARATOR + base
            with open(data_path, 'wb') as fd:
                fd.write(modified)
            return data_path
    g.dir = os.path.dirname(data_path)
    return g

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'python experiment.py input_data output_dir'
        exit(1)
    base_path = sys.argv[1]
    data_dir = sys.argv[2]
    try:
        os.makedirs(data_dir)
    except OSError:
        # Directory already exists
        pass
    data_path = os.path.join(data_dir, 'data.txt')
    e = Experiment(
        'identity',
        modifier_factory(base_path, data_path)
    )
    e.run(1)
