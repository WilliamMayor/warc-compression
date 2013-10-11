"""
Removes a single character from the file at a time until the file is empty
"""
import os
from warcompress.experiments.Experiment import Experiment
from warcompress.experiments import modifiers


def factory_factory(base_path, data_path):
    def g():
        with open(base_path, 'r') as fd:
            base_text = fd.read()
        text = base_text
        previous = base_text
        while len(previous) > 0:
            previous = modifiers.delete(previous, 1)
            text += Experiment.RECORD_SEPARATOR + previous
        with open(data_path, 'w') as fd:
            fd.write(text)
        return data_path
    g.dir = os.path.dirname(data_path)
    return g

if __name__ == '__main__':
    script_path = os.path.realpath(__file__)
    script_dir = os.path.dirname(script_path)
    data_dir = os.path.join(script_dir, 'one_char_delete')
    try:
        os.mkdir(data_dir)
    except OSError:
        # Directory already exists
        pass
    data_path = os.path.join(data_dir, 'data.txt')
    base_path = os.path.join(script_dir, 'base.txt')
    e = Experiment(
        'one_char_delete',
        factory_factory(base_path, data_path)
    )
    e.run(1)
