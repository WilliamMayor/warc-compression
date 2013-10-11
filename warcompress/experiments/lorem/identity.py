"""
How much compression can you get on a single file?
"""
import os
import shutil
from warcompress.experiments.Experiment import Experiment


def factory_factory(path):
    def g():
        return path
    g.dir = os.path.dirname(path)
    return g

if __name__ == '__main__':
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    data_dir = os.path.join(script_dir, 'identity')
    try:
        os.mkdir(data_dir)
    except OSError:
        # Directory already exists
        pass
    data_path = os.path.join(data_dir, 'data.txt')
    shutil.copyfile(os.path.join(script_dir, 'base.txt'), data_path)
    e = Experiment(
        'identity',
        factory_factory(data_path)
    )
    e.run(1)
