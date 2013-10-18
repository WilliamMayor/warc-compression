"""
Removes a single character at random from the file 20 times
"""
import os
import sys
from warcompress.experiments.Experiment import Experiment
from warcompress.experiments import modifiers
from warcompress.encodings import utilities


def modifier_factory(base_path, data_path):
    def g():
        with open(base_path, 'r') as fd:
            base_text = fd.read()
        text = base_text
        previous = base_text
        for i in xrange(0, 20):
            previous = modifiers.delete(previous, 1)
            text += utilities.RECORD_SEPARATOR + previous
        with open(data_path, 'w') as fd:
            fd.write(text)
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
        'one_char_delete',
        modifier_factory(base_path, data_path)
    )
    e.run(1)
