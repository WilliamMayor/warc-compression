"""
How much compression can you get on a single file?
"""
import os
import sys
import shutil
from warcompress.experiments.Experiment import Experiment


def modifier_factory(path):
    def g():
        return path
    g.dir = os.path.dirname(path)
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
    shutil.copyfile(base_path, data_path)
    e = Experiment(
        'identity',
        modifier_factory(data_path)
    )
    e.run(1)
