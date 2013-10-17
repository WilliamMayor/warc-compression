"""
How much compression can you get on a single file?
"""
import os
import sys
import shutil
from warcompress.experiments.Experiment import Experiment


def factory_factory(path):
    def g():
        return path
    g.dir = os.path.dirname(path)
    return g

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'Must provide base data to work from'
        exit(1)
    base_text_path = sys.argv[1]
    base_text_dir = os.path.dirname(base_text_path)
    data_dir = os.path.join(base_text_dir, 'identity')
    try:
        os.mkdir(data_dir)
    except OSError:
        # Directory already exists
        pass
    data_path = os.path.join(data_dir, 'data.txt')
    shutil.copyfile(base_text_path, data_path)
    e = Experiment(
        'identity',
        factory_factory(data_path)
    )
    e.run(1)
